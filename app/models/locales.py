from app.db import get_connection
import pandas as pd
import sqlite3


def get_locales_catalogo() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT id, numero, nombre FROM reception_local ORDER BY numero", conn)
    finally:
        conn.close()
    return df


def obtener_locales():
    conn = get_connection()
    try:
        rows = conn.execute("SELECT id, numero, nombre FROM reception_local ORDER BY numero").fetchall()
        return rows
    finally:
        conn.close()


def numero_existe(numero: int, exclude_id: int | None = None) -> bool:
    conn = get_connection()
    try:
        if exclude_id is None:
            row = conn.execute("SELECT 1 FROM reception_local WHERE numero = ? LIMIT 1", (numero,)).fetchone()
        else:
            row = conn.execute("SELECT 1 FROM reception_local WHERE numero = ? AND id <> ? LIMIT 1", (numero, exclude_id)).fetchone()
        return row is not None
    finally:
        conn.close()


def siguiente_numero_disponible() -> int:
    conn = get_connection()
    try:
        row = conn.execute("SELECT MAX(numero) FROM reception_local").fetchone()
        max_num = row[0] if row and row[0] is not None else 0
        return int(max_num) + 1
    finally:
        conn.close()


def agregar_local(numero: int, nombre: str):
    conn = get_connection()
    try:
        if numero_existe(numero):
            return False, "Ya existe un local con ese número."
        conn.execute("INSERT INTO reception_local (numero, nombre) VALUES (?, ?)", (numero, nombre))
        conn.commit(); return True, "Local agregado correctamente."
    except sqlite3.IntegrityError:
        return False, "Ya existe un local con ese número o nombre."
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def editar_local(id: int, numero: int, nombre: str):
    conn = get_connection()
    try:
        if numero_existe(numero, exclude_id=id):
            return False, "No se puede asignar un número que ya está usado por otro local."
        conn.execute("UPDATE reception_local SET numero=?, nombre=? WHERE id=?", (numero, nombre, id))
        conn.commit(); return True, "Local editado correctamente."
    except sqlite3.IntegrityError:
        return False, "Ya existe un local con ese número o nombre."
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def eliminar_local(id: int):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM reception_local WHERE id=?", (id,))
        conn.commit(); return True, "Local eliminado correctamente."
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()
