# Database helpers
import os
import sqlite3
from .config import get_db_path


def get_connection():
    """Create and return a SQLite connection, ensuring path is valid."""
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    # Enforce foreign key constraints
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    # quick sanity
    conn.execute("SELECT 1").fetchone()
    return conn


def init_database():
    conn = get_connection()
    c = conn.cursor()

    # choferes
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS choferes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE,
            contacto TEXT,
            fecha_registro DATE DEFAULT CURRENT_DATE
        )
        """
    )

    # viajes
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS viajes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chofer_id INTEGER NOT NULL,
            fecha_viaje DATE NOT NULL,
            estado TEXT DEFAULT 'En Curso',
            FOREIGN KEY (chofer_id) REFERENCES choferes (id)
        )
        """
    )

    # viaje_locales
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS viaje_locales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            viaje_id INTEGER NOT NULL,
            numero_local TEXT NOT NULL,
            cajas_enviadas INTEGER NOT NULL,
            cajas_devueltas INTEGER DEFAULT 0,
            FOREIGN KEY (viaje_id) REFERENCES viajes (id)
        )
        """
    )

    # reception_local
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS reception_local (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero INTEGER UNIQUE,
            nombre TEXT UNIQUE
        )
        """
    )

    # users (asegurar que exista para evitar depender solo de ensure_user_table fallback)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'admin'
        )
        """
    )

    # devoluciones_log (historial de devoluciones individuales y masivas)
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS devoluciones_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            viaje_id INTEGER NOT NULL,
            viaje_local_id INTEGER NOT NULL,
            numero_local TEXT NOT NULL,
            cantidad INTEGER NOT NULL,
            tipo TEXT NOT NULL DEFAULT 'individual', -- 'individual' | 'masiva'
            usuario TEXT, -- username que registró la devolución
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (viaje_id) REFERENCES viajes (id),
            FOREIGN KEY (viaje_local_id) REFERENCES viaje_locales (id)
        )
        """
    )

    conn.commit()
    # Índices idempotentes (performance)
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_viajes_fecha ON viajes(fecha_viaje)",
        "CREATE INDEX IF NOT EXISTS idx_devlog_created ON devoluciones_log(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_cd_despachos_fecha ON cd_despachos(fecha)",
        "CREATE INDEX IF NOT EXISTS idx_cd_envios_fecha ON cd_envios_origen(fecha)",
        "CREATE INDEX IF NOT EXISTS idx_viaje_locales_viaje ON viaje_locales(viaje_id)",
        "CREATE INDEX IF NOT EXISTS idx_devlog_viaje ON devoluciones_log(viaje_id)",
        "CREATE INDEX IF NOT EXISTS idx_devlog_viaje_local ON devoluciones_log(viaje_local_id)"
    ]
    for stmt in indices:
        try:
            c.execute(stmt)
        except Exception:
            pass
    conn.commit()
    conn.close()
