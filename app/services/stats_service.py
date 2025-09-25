"""Servicios de estadísticas y agregaciones para el dashboard.
Separado para reducir tamaño del archivo principal y facilitar pruebas.
"""
from __future__ import annotations
import pandas as pd
from app.db import get_connection


def get_dashboard_stats() -> dict:
    """Devuelve métricas globales para el dashboard principal.
    Retorna siempre claves: total_choferes, viajes_activos, total_enviadas, total_devueltas, pendientes.
    """
    conn = get_connection(); cur = conn.cursor()
    try:
        total_choferes = cur.execute("SELECT COUNT(*) FROM choferes").fetchone()[0]
        viajes_activos = cur.execute("SELECT COUNT(*) FROM viajes WHERE estado='En Curso'").fetchone()[0]
        row = cur.execute("SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM viaje_locales").fetchone()
        total_enviadas = int((row[0] or 0) if row else 0)
        total_devueltas = int((row[1] or 0) if row else 0)
        pendientes = total_enviadas - total_devueltas
        if pendientes < 0:
            pendientes = 0
        return {
            'total_choferes': int(total_choferes or 0),
            'viajes_activos': int(viajes_activos or 0),
            'total_enviadas': total_enviadas,
            'total_devueltas': total_devueltas,
            'pendientes': pendientes,
        }
    finally:
        conn.close()


def get_pendientes_por_local() -> pd.DataFrame:
    """Devuelve DataFrame con columnas: local, enviadas, devueltas, pendientes."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT numero_local AS local, SUM(cajas_enviadas) AS enviadas, SUM(cajas_devueltas) AS devueltas FROM viaje_locales GROUP BY numero_local",
            conn
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df['enviadas'] = df['enviadas'].fillna(0).astype(int)
    df['devueltas'] = df['devueltas'].fillna(0).astype(int)
    df['pendientes'] = df['enviadas'] - df['devueltas']
    return df
