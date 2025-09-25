import hashlib
import pandas as pd
from app.db import get_connection


VALID_ROLES = ["admin", "cd_only", "no_cd_edit"]


def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def ensure_user_table():
    conn = get_connection(); cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'admin'
        )
        """
    )
    cur.execute("SELECT 1 FROM users WHERE username='admin' LIMIT 1")
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?,?,?)",
            ("admin", hash_password("admin"), "admin")
        )
    conn.commit(); conn.close()


def get_user(username: str):
    # Normalizar posibles espacios accidentales
    if username:
        username = username.strip()
    conn = get_connection(); cur = conn.cursor()
    cur.execute("SELECT id, username, password_hash, role FROM users WHERE username=?", (username,))
    row = cur.fetchone(); conn.close()
    if not row:
        return None
    return {"id": row[0], "username": row[1], "password_hash": row[2], "role": row[3]}


def verify_password(plain: str, hashed: str) -> bool:
    return hash_password(plain) == hashed


def set_user_password(user_id: int, new_password: str):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(new_password), user_id))
    conn.commit(); conn.close(); return True, "Contraseña actualizada"


def create_user(username: str, password: str, role: str):
    if username:
        username = username.strip()
    if role not in VALID_ROLES:
        return False, "Rol inválido"
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?,?,?)",
            (username, hash_password(password), role)
        )
        conn.commit(); conn.close(); return True, "Usuario creado"
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            return False, "El usuario ya existe"
        return False, f"Error: {e}"


def list_users() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT id, username, role FROM users ORDER BY username", conn)
    except Exception:
        df = pd.DataFrame(columns=["id","username","role"])
    finally:
        conn.close()
    return df


def update_user(user_id: int, new_username: str | None = None, new_role: str | None = None):
    if new_username:
        new_username = new_username.strip()
    if new_role and new_role not in VALID_ROLES:
        return False, "Rol inválido"
    sets = []; params = []
    if new_username:
        sets.append("username = ?"); params.append(new_username)
    if new_role:
        sets.append("role = ?"); params.append(new_role)
    if not sets:
        return False, "Sin cambios"
    params.append(user_id)
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit(); conn.close(); return True, "Usuario actualizado"
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            return False, "Nombre de usuario duplicado"
        return False, f"Error: {e}"


def delete_user(user_id: int):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("SELECT role FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    if not row:
        conn.close(); return False, "Usuario no existe"
    role = row[0]
    if role == 'admin':
        cur.execute("SELECT COUNT(*) FROM users WHERE role='admin'")
        if cur.fetchone()[0] <= 1:
            conn.close(); return False, "Debe existir al menos un admin"
    cur.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit(); conn.close(); return True, "Usuario eliminado"
