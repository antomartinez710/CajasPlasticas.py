from app.db import get_connection
import pandas as pd


def get_choferes() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM choferes", conn)
    conn.close()
    return df


def agregar_chofer(nombre: str, contacto: str | None = None):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO choferes (nombre, contacto) VALUES (?,?)", (nombre, contacto))
        conn.commit(); return True, "Chofer agregado exitosamente ✅"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def eliminar_chofer(chofer_id: int):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("DELETE FROM choferes WHERE id = ?", (chofer_id,))
    conn.commit(); conn.close()
