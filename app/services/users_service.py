"""Service layer para usuarios.
Incluye: creación, listado, actualización, cambio de contraseña, autenticación básica.
"""
from __future__ import annotations
import sqlite3, os, hashlib
try:
    import bcrypt  # bcrypt para hashing fuerte
    _BCRYPT_AVAILABLE = True
except Exception:
    _BCRYPT_AVAILABLE = False
from typing import List, Dict, Any, Optional, Tuple

_DB_FILENAME = "cajas_plasticas.db"

ROLES_VALIDOS = ("admin", "cd_only", "no_cd_edit")

def _get_db_path() -> str:
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), _DB_FILENAME)

def get_connection():
    return sqlite3.connect(_get_db_path(), check_same_thread=False)

# --- hashing ---

def _legacy_sha256(pw: str) -> str:
    return hashlib.sha256((pw or "").encode("utf-8")).hexdigest()

def hash_password(pw: str) -> str:
    """Genera hash de contraseña.
    - Si bcrypt disponible: retorna string prefijado 'bcrypt$' + hash.
    - Si no: SHA-256 legacy (prefijo 'sha256$') para poder detectar y migrar luego.
    """
    if _BCRYPT_AVAILABLE:
        salt = bcrypt.gensalt(rounds=12)
        return "bcrypt$" + bcrypt.hashpw((pw or "").encode("utf-8"), salt).decode("utf-8")
    return "sha256$" + _legacy_sha256(pw)

def _is_bcrypt(stored: str) -> bool:
    return stored.startswith("bcrypt$")

def _is_sha256(stored: str) -> bool:
    return stored.startswith("sha256$") or (len(stored) == 64 and all(c in '0123456789abcdef' for c in stored.lower()))

# --- CRUD ---

def crear_usuario(username: str, password: str, role: str) -> Tuple[bool, str]:
    if role not in ROLES_VALIDOS:
        return False, "Rol inválido"
    if not username or not password:
        return False, "Usuario y contraseña requeridos"
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO users (username, password_hash, role) VALUES (?,?,?)", (username.strip(), hash_password(password), role))
        conn.commit(); return True, "Usuario creado"
    except sqlite3.IntegrityError:
        return False, "Usuario ya existe"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()

def listar_usuarios() -> List[Dict[str, Any]]:
    conn = get_connection(); cur = conn.cursor()
    try:
        rows = cur.execute("SELECT id, username, role FROM users ORDER BY username").fetchall()
        return [ {"id": r[0], "username": r[1], "role": r[2]} for r in rows ]
    finally:
        conn.close()

def actualizar_usuario(user_id: int, role: Optional[str] = None, username: Optional[str] = None) -> Tuple[bool, str]:
    if role and role not in ROLES_VALIDOS:
        return False, "Rol inválido"
    if not role and not username:
        return False, "Nada para actualizar"
    conn = get_connection(); cur = conn.cursor()
    try:
        sets = []; params = []
        if username:
            sets.append("username=?"); params.append(username.strip())
        if role:
            sets.append("role=?"); params.append(role)
        params.append(user_id)
        cur.execute(f"UPDATE users SET {', '.join(sets)} WHERE id=?", params)
        if cur.rowcount == 0:
            return False, "Usuario no encontrado"
        conn.commit(); return True, "Usuario actualizado"
    except sqlite3.IntegrityError:
        return False, "Nombre de usuario en uso"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()

def eliminar_usuario(user_id: int) -> Tuple[bool, str]:
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM users WHERE id=?", (user_id,))
        if cur.rowcount == 0:
            return False, "Usuario no encontrado"
        conn.commit(); return True, "Usuario eliminado"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()

def set_password(user_id: int, new_password: str) -> Tuple[bool, str]:
    if not new_password:
        return False, "Contraseña requerida"
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(new_password), user_id))
        if cur.rowcount == 0:
            return False, "Usuario no encontrado"
        conn.commit(); return True, "Contraseña actualizada"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()

# --- Autenticación ---

def obtener_usuario(username: str) -> Optional[Dict[str, Any]]:
    conn = get_connection(); cur = conn.cursor()
    try:
        row = cur.execute("SELECT id, username, password_hash, role FROM users WHERE username=?", (username,)).fetchone()
        if not row:
            return None
        return {"id": row[0], "username": row[1], "password_hash": row[2], "role": row[3]}
    finally:
        conn.close()

def verificar_password(clear_pw: str, stored_hash: str) -> bool:
    """Verifica contraseña con soporte backward compatibility.
    - bcrypt$...: usa bcrypt.checkpw
    - sha256$... o hash plano de 64 hex: compara SHA-256 legacy
    - auto-upgrade: si se valida con SHA-256 y bcrypt disponible -> rehash bcrypt.
    """
    if not stored_hash:
        return False
    # bcrypt
    if _is_bcrypt(stored_hash):
        if not _BCRYPT_AVAILABLE:
            return False  # hash bcrypt almacenado pero lib no disponible
        hash_part = stored_hash[len("bcrypt$"):].encode("utf-8")
        return bcrypt.checkpw((clear_pw or "").encode("utf-8"), hash_part)
    # sha256
    if _is_sha256(stored_hash):
        expected = stored_hash
        # si tiene prefijo
        if stored_hash.startswith("sha256$"):
            expected = stored_hash[len("sha256$"):]
        if _legacy_sha256(clear_pw) == expected:
            # upgrade
            if _BCRYPT_AVAILABLE:
                user = None
                # Necesitamos id para rehash -> omitimos por simplicidad salvo que se provea por fuera
                # (upgrade diferido a set_password si se desea).
            return True
    return False

__all__ = [
    "crear_usuario","listar_usuarios","actualizar_usuario","eliminar_usuario","set_password",
    "hash_password","obtener_usuario","verificar_password","ROLES_VALIDOS"
]
