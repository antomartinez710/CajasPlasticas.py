"""Service layer para gestión de locales.

Responsabilidades:
- Encapsular CRUD sobre tabla reception_local
- Proveer helpers de validación (numero_existe, siguiente_numero)
- Retornar resultados consistentes (ok, msg / data)
"""
from __future__ import annotations
import sqlite3
from typing import List, Optional, Tuple, Dict, Any
import os

# Conexión reutilizable
_DB_FILENAME = "cajas_plasticas.db"

def _get_db_path() -> str:
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), _DB_FILENAME)

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(_get_db_path(), check_same_thread=False)

# --- helpers internos ---

def numero_existe(numero: int, exclude_id: Optional[int] = None) -> bool:
    conn = get_connection(); cur = conn.cursor()
    try:
        if exclude_id is None:
            row = cur.execute("SELECT 1 FROM reception_local WHERE numero=? LIMIT 1", (numero,)).fetchone()
        else:
            row = cur.execute("SELECT 1 FROM reception_local WHERE numero=? AND id<>? LIMIT 1", (numero, exclude_id)).fetchone()
        return row is not None
    finally:
        conn.close()

def siguiente_numero() -> int:
    conn = get_connection(); cur = conn.cursor()
    try:
        row = cur.execute("SELECT MAX(numero) FROM reception_local").fetchone()
        max_num = row[0] if row and row[0] is not None else 0
        return int(max_num) + 1
    finally:
        conn.close()

# --- CRUD ---

def listar_locales() -> List[Dict[str, Any]]:
    conn = get_connection(); cur = conn.cursor()
    try:
        rows = cur.execute("SELECT id, numero, nombre FROM reception_local ORDER BY numero").fetchall()
        return [ {"id": r[0], "numero": r[1], "nombre": r[2]} for r in rows ]
    finally:
        conn.close()

def crear_local(numero: int, nombre: str) -> Tuple[bool, str]:
    if numero_existe(numero):
        return False, "Ya existe un local con ese número"
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO reception_local (numero, nombre) VALUES (?, ?)", (numero, nombre.strip()))
        conn.commit()
        return True, "Local agregado correctamente"
    except sqlite3.IntegrityError:
        return False, "Número o nombre duplicado"
    except Exception as e:
        return False, f"Error inesperado: {e}"
    finally:
        conn.close()

def actualizar_local(id_local: int, numero: int, nombre: str) -> Tuple[bool, str]:
    if numero_existe(numero, exclude_id=id_local):
        return False, "No se puede asignar un número ya usado"
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("UPDATE reception_local SET numero=?, nombre=? WHERE id=?", (numero, nombre.strip(), id_local))
        if cur.rowcount == 0:
            return False, "Local no encontrado"
        conn.commit()
        return True, "Local editado correctamente"
    except sqlite3.IntegrityError:
        return False, "Número o nombre duplicado"
    except Exception as e:
        return False, f"Error inesperado: {e}"
    finally:
        conn.close()

def eliminar_local(id_local: int) -> Tuple[bool, str]:
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM reception_local WHERE id=?", (id_local,))
        if cur.rowcount == 0:
            return False, "Local no encontrado"
        conn.commit()
        return True, "Local eliminado correctamente"
    except Exception as e:
        return False, f"Error inesperado: {e}"
    finally:
        conn.close()

# --- API amigable para UI ---

def get_catalogo_con_display():
    data = listar_locales()
    # Añadir campo display reutilizable
    for d in data:
        d["display"] = f"{d['numero']} - {d['nombre']}" if d['nombre'] else str(d['numero'])
    return data

__all__ = [
    "listar_locales","crear_local","actualizar_local","eliminar_local",
    "numero_existe","siguiente_numero","get_catalogo_con_display"
]
