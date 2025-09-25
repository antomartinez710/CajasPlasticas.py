"""Servicio de viajes y devoluciones.
Encapsula la lógica de dominio y acceso a datos para:
- listar viajes y sus locales
- crear viajes
- registrar devoluciones (individual y masiva)
- consultar / editar / eliminar registros del log de devoluciones
- actualizar estado y eliminar viajes
Usa modelos si están disponibles (para futura modularización completa) y cae a SQL directo como fallback.
"""
from __future__ import annotations
import pandas as pd
from typing import Optional, Iterable
from app.db import get_connection

# Imports opcionales de modelos (si existen) -----------------
try:
    from app.models.viajes import (
        get_viajes_detallados as mdl_get_viajes_detallados,
        get_viaje_locales as mdl_get_viaje_locales,
        registrar_devolucion as mdl_registrar_devolucion,
        registrar_devolucion_todas_por_viaje as mdl_registrar_devolucion_todas_por_viaje,
        crear_viaje as mdl_crear_viaje,
        eliminar_viaje as mdl_eliminar_viaje,
        actualizar_estado_viaje as mdl_actualizar_estado_viaje,
    )
except Exception:  # pragma: no cover - tolerancia de import
    mdl_get_viajes_detallados = None
    mdl_get_viaje_locales = None
    mdl_registrar_devolucion = None
    mdl_registrar_devolucion_todas_por_viaje = None
    mdl_crear_viaje = None
    mdl_eliminar_viaje = None
    mdl_actualizar_estado_viaje = None

# Viajes -----------------------------------------------------

def listar_viajes(fecha_desde=None, fecha_hasta=None, chofer_id=None, estado=None) -> pd.DataFrame:
    if mdl_get_viajes_detallados:
        try:
            return mdl_get_viajes_detallados(fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, chofer_id=chofer_id, estado=estado)
        except Exception:
            pass
    conn = get_connection()
    params = []
    query = """
        SELECT v.id, v.fecha_viaje, v.estado, c.nombre as chofer,
               COUNT(vl.id) AS total_locales,
               SUM(vl.cajas_enviadas) AS total_enviadas,
               SUM(vl.cajas_devueltas) AS total_devueltas,
               SUM(vl.cajas_enviadas - vl.cajas_devueltas) AS pendientes
        FROM viajes v
        LEFT JOIN choferes c ON v.chofer_id = c.id
        LEFT JOIN viaje_locales vl ON vl.viaje_id = v.id
        WHERE 1=1
    """
    if fecha_desde:
        query += " AND date(v.fecha_viaje) >= date(?)"; params.append(fecha_desde)
    if fecha_hasta:
        query += " AND date(v.fecha_viaje) <= date(?)"; params.append(fecha_hasta)
    if chofer_id is not None:
        query += " AND v.chofer_id = ?"; params.append(chofer_id)
    if estado and estado != "Todos":
        query += " AND v.estado = ?"; params.append(estado)
    query += " GROUP BY v.id ORDER BY v.fecha_viaje DESC, v.id DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    if df.empty:
        return df
    for col in ["total_locales","total_enviadas","total_devueltas","pendientes"]:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    return df

def viaje_locales(viaje_id: int) -> pd.DataFrame:
    if mdl_get_viaje_locales:
        try:
            return mdl_get_viaje_locales(viaje_id)
        except Exception:
            pass
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT id, numero_local, cajas_enviadas, cajas_devueltas FROM viaje_locales WHERE viaje_id = ? ORDER BY id",
        conn,
        params=[viaje_id]
    )
    conn.close()
    return df

def crear_viaje(chofer_id: int, fecha_viaje, locales: Iterable[dict]):
    if mdl_crear_viaje:
        try:
            return mdl_crear_viaje(chofer_id, fecha_viaje, locales)
        except Exception:
            pass
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO viajes (chofer_id, fecha_viaje, estado) VALUES (?,?,?)", (chofer_id, fecha_viaje, 'En Curso'))
        viaje_id = cur.lastrowid
        for local in locales:
            num = local.get('numero_local'); cajas = int(local.get('cajas_enviadas') or 0)
            if num and cajas > 0:
                cur.execute(
                    "INSERT INTO viaje_locales (viaje_id, numero_local, cajas_enviadas, cajas_devueltas) VALUES (?,?,?,0)",
                    (viaje_id, num, cajas)
                )
        conn.commit(); return viaje_id
    finally:
        conn.close()

def eliminar_viaje(viaje_id: int):
    if mdl_eliminar_viaje:
        try:
            return mdl_eliminar_viaje(viaje_id)
        except Exception:
            pass
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM viaje_locales WHERE viaje_id = ?", (viaje_id,))
        cur.execute("DELETE FROM viajes WHERE id = ?", (viaje_id,))
        conn.commit(); return True
    except Exception:
        return False
    finally:
        conn.close()

def actualizar_estado_viaje(viaje_id: int, nuevo_estado: str):
    if mdl_actualizar_estado_viaje:
        try:
            return mdl_actualizar_estado_viaje(viaje_id, nuevo_estado)
        except Exception:
            pass
    conn = get_connection(); cur = conn.cursor()
    cur.execute("UPDATE viajes SET estado = ? WHERE id = ?", (nuevo_estado, viaje_id))
    conn.commit(); conn.close(); return True

# Devoluciones ------------------------------------------------

def registrar_devolucion(viaje_local_id: int, cantidad: int, usuario: Optional[str] = None):
    if mdl_registrar_devolucion:
        try:
            return mdl_registrar_devolucion(viaje_local_id, cantidad, usuario=usuario)
        except Exception:
            pass
    cantidad = int(cantidad)
    if cantidad <= 0:
        return False, "Cantidad inválida"
    conn = get_connection(); cur = conn.cursor()
    try:
        row = cur.execute("SELECT viaje_id, cajas_enviadas, cajas_devueltas, numero_local FROM viaje_locales WHERE id=?", (viaje_local_id,)).fetchone()
        if not row:
            return False, "Ítem de viaje no encontrado"
        viaje_id, envi, dev, numero_local = row
        pend = (envi or 0) - (dev or 0)
        if pend <= 0:
            return False, "Sin pendientes"
        aplicar = min(pend, cantidad)
        cur.execute("UPDATE viaje_locales SET cajas_devueltas = cajas_devueltas + ? WHERE id=?", (aplicar, viaje_local_id))
        cur.execute(
            "INSERT INTO devoluciones_log (viaje_id, viaje_local_id, numero_local, cantidad, tipo, usuario) VALUES (?,?,?,?,?,?)",
            (viaje_id, viaje_local_id, numero_local, aplicar, 'manual', usuario)
        )
        conn.commit(); return True, f"Registradas {aplicar} cajas"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()

def registrar_devolucion_todas_por_viaje(viaje_id: int, usuario: Optional[str] = None):
    if mdl_registrar_devolucion_todas_por_viaje:
        try:
            return mdl_registrar_devolucion_todas_por_viaje(viaje_id, usuario=usuario)
        except Exception:
            pass
    conn = get_connection(); cur = conn.cursor()
    try:
        rows = cur.execute("SELECT id, cajas_enviadas, cajas_devueltas, numero_local FROM viaje_locales WHERE viaje_id=?", (viaje_id,)).fetchall()
        total = 0
        for rid, envi, dev, numero_local in rows:
            pend = (envi or 0) - (dev or 0)
            if pend > 0:
                cur.execute("UPDATE viaje_locales SET cajas_devueltas = cajas_devueltas + ? WHERE id=?", (pend, rid))
                cur.execute(
                    "INSERT INTO devoluciones_log (viaje_id, viaje_local_id, numero_local, cantidad, tipo, usuario) VALUES (?,?,?,?,?,?)",
                    (viaje_id, rid, numero_local, pend, 'masiva', usuario)
                )
                total += pend
        conn.commit(); return True, f"{total} cajas registradas"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()

def update_devueltas_viaje_locales(viaje_id: int, items: list[dict]):
    """Actualiza en lote cajas_devueltas para los locales del viaje.
    items: [{'id': viaje_local_id, 'cajas_devueltas': int}]
    Reglas:
      - 0 <= cajas_devueltas <= cajas_enviadas
      - Ignora ítems sin cambios
      - Operación atómica
    No registra log adicional (solo conserva los logs de devoluciones previas).
    """
    if not items:
        return False, "Sin cambios"
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        actualizados = 0
        for it in items:
            try:
                rid = int(it.get('id'))
                dev_new = int(it.get('cajas_devueltas', 0))
            except Exception:
                conn.rollback(); return False, "Datos inválidos"
            row = cur.execute("SELECT cajas_enviadas, cajas_devueltas FROM viaje_locales WHERE id=? AND viaje_id=?", (rid, viaje_id)).fetchone()
            if not row:
                conn.rollback(); return False, f"Item {rid} no pertenece al viaje"
            envi, dev_old = row
            envi = int(envi or 0); dev_old = int(dev_old or 0)
            if dev_new < 0 or dev_new > envi:
                conn.rollback(); return False, f"Devueltas inválidas para item {rid}"
            if dev_new != dev_old:
                cur.execute("UPDATE viaje_locales SET cajas_devueltas=? WHERE id=?", (dev_new, rid))
                actualizados += 1
        conn.commit();
        if actualizados == 0:
            return False, "Sin cambios"
        return True, f"{actualizados} locales actualizados"
    except Exception as e:
        conn.rollback(); return False, f"Error: {e}"
    finally:
        conn.close()



