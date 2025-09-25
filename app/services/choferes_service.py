"""Service layer para Choferes.
Encapsula acceso a la tabla choferes.
"""
from __future__ import annotations
import sqlite3, os
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd

_DB_FILENAME = "cajas_plasticas.db"

def _get_db_path() -> str:
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), _DB_FILENAME)

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(_get_db_path(), check_same_thread=False)

# --- Query Helpers ---

def listar_choferes(as_dataframe: bool = True):
    conn = get_connection()
    try:
        if as_dataframe:
            return pd.read_sql_query("SELECT id, nombre, contacto FROM choferes ORDER BY nombre", conn)
        else:
            cur = conn.cursor()
            rows = cur.execute("SELECT id, nombre, contacto FROM choferes ORDER BY nombre").fetchall()
            return [ {"id": r[0], "nombre": r[1], "contacto": r[2]} for r in rows ]
    finally:
        conn.close()

def crear_chofer(nombre: str, contacto: Optional[str] = None) -> Tuple[bool, str]:
    if not nombre or not nombre.strip():
        return False, "El nombre es requerido"
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO choferes (nombre, contacto) VALUES (?, ?)", (nombre.strip(), contacto.strip() if contacto else None))
        conn.commit()
        return True, "Chofer agregado exitosamente ✅"
    except sqlite3.IntegrityError:
        return False, "Duplicado (integridad)"
    except Exception as e:
        return False, f"Error inesperado: {e}"
    finally:
        conn.close()

def eliminar_chofer(chofer_id: int) -> Tuple[bool, str]:
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM choferes WHERE id=?", (chofer_id,))
        if cur.rowcount == 0:
            return False, "Chofer no encontrado"
        conn.commit()
        return True, "Chofer eliminado"
    except Exception as e:
        return False, f"Error inesperado: {e}"
    finally:
        conn.close()

__all__ = [
    "listar_choferes","crear_chofer","eliminar_chofer"
]
