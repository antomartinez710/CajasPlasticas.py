import pandas as pd
from typing import Optional, Tuple
from datetime import date

try:
    from app.db import get_connection as get_connection
except Exception:  # fallback minimal
    import sqlite3, os
    def get_connection():
        return sqlite3.connect(os.getenv("CAJAS_PLASTICAS_DB", "cajas_plasticas.db"))

try:
    from app.models.locales import get_locales_catalogo as mdl_get_locales_catalogo
except Exception:
    mdl_get_locales_catalogo = None

# Helper interno para obtener catálogo de locales

def _get_locales_catalogo():
    if mdl_get_locales_catalogo:
        try:
            return mdl_get_locales_catalogo()
        except Exception:
            pass
    # fallback directo a la BD (mínimo)
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT numero, nombre FROM locales ORDER BY numero", conn)
    except Exception:
        df = pd.DataFrame(columns=["numero", "nombre"])
    finally:
        conn.close()
    return df


def _get_cd_display():
    catalogo_df = _get_locales_catalogo()
    if catalogo_df.empty:
        return None
    for _, row in catalogo_df.iterrows():
        nombre = str(row.get("nombre") or "")
        if nombre and ("cd" in nombre.lower()):
            numero = row.get("numero")
            return f"{numero} - {nombre}" if nombre else str(numero)
    return None

# =====================
# CONSULTAS / RESÚMENES
# =====================

def cd_resumen_por_cd():
    """Resumen por cada CD: enviadas, devueltas y pendientes."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT cd_local,
                   SUM(cajas_enviadas) AS enviadas,
                   SUM(cajas_devueltas) AS devueltas,
                   SUM(cajas_enviadas - cajas_devueltas) AS pendientes
            FROM cd_despachos
            GROUP BY cd_local
            HAVING enviadas > 0
            ORDER BY pendientes DESC
            """,
            conn
        )
        return df
    finally:
        conn.close()


def cd_totales():
    """Calcula totales y stock del CD detectado automáticamente."""
    cd_disp = _get_cd_display()
    recibido_viajes = 0
    if cd_disp:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT SUM(cajas_enviadas) FROM viaje_locales WHERE numero_local = ?",
                (cd_disp,)
            ).fetchone()
            recibido_viajes = (row[0] or 0) if row else 0
        finally:
            conn.close()
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM cd_despachos"
        ).fetchone()
        enviados_cd = (row[0] or 0) if row else 0
        devueltos = (row[1] or 0) if row else 0
        row2 = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM cd_envios_origen"
        ).fetchone()
        enviados_origen = (row2[0] or 0) if row2 else 0
    finally:
        conn.close()
    entradas_totales = recibido_viajes + devueltos
    stock = entradas_totales - enviados_cd - enviados_origen
    if stock < 0:
        stock = 0
    return {
        "cd": cd_disp,
        "recibido_viajes": int(recibido_viajes),
        "entradas_totales": int(entradas_totales),
        "enviados": int(enviados_cd),
        "enviados_origen": int(enviados_origen),
        "devueltos": int(devueltos),
        "stock": int(stock),
    }

# =====================
# ENVÍOS A ORIGEN
# =====================

def cd_enviar_a_origen(fecha, cajas: int) -> Tuple[bool, str]:
    cajas = int(cajas)
    if cajas <= 0:
        return False, "La cantidad debe ser mayor a 0"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cd_disp = _get_cd_display()
        row_rec = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM viaje_locales WHERE numero_local = ?",
            (cd_disp,)
        ).fetchone() if cd_disp else (0,)
        recibido = (row_rec[0] or 0)
        row_cd = conn.execute(
            "SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM cd_despachos"
        ).fetchone()
        enviados_cd = (row_cd[0] or 0) if row_cd else 0
        devueltos = (row_cd[1] or 0) if row_cd else 0
        row_ori = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM cd_envios_origen"
        ).fetchone()
        enviados_origen = (row_ori[0] or 0) if row_ori else 0
        entradas_totales = recibido + devueltos
        stock = entradas_totales - enviados_cd - enviados_origen
        if cajas > stock:
            conn.execute("ROLLBACK")
            return False, f"Stock insuficiente. Disponible: {int(stock)}"
        conn.execute(
            "INSERT INTO cd_envios_origen (fecha, cajas_enviadas) VALUES (?, ?)",
            (fecha, cajas)
        )
        conn.commit()
        return True, "Envío al origen registrado"
    except Exception as e:
        try: conn.execute("ROLLBACK")
        except Exception: pass
        return False, f"Error al registrar envío al origen: {e}"
    finally:
        conn.close()

def cd_listar_envios_origen(start_date=None, end_date=None):
    conn = get_connection()
    try:
        query = "SELECT id, fecha, cajas_enviadas FROM cd_envios_origen WHERE 1=1"
        params = []
        if start_date:
            query += " AND date(fecha) >= date(?)"; params.append(start_date)
        if end_date:
            query += " AND date(fecha) <= date(?)"; params.append(end_date)
        query += " ORDER BY fecha DESC, id DESC"
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

def cd_actualizar_envio_origen(envio_id, nueva_fecha, nuevas_cajas):
    nuevas_cajas = int(nuevas_cajas)
    if nuevas_cajas <= 0:
        return False, "La cantidad debe ser mayor a 0"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row_cur = conn.execute(
            "SELECT cajas_enviadas FROM cd_envios_origen WHERE id = ?",
            (envio_id,)
        ).fetchone()
        if not row_cur:
            conn.execute("ROLLBACK"); return False, "Envío no encontrado"
        old_cajas = int(row_cur[0] or 0)
        cd_disp = _get_cd_display()
        row_rec = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM viaje_locales WHERE numero_local = ?",
            (cd_disp,)
        ).fetchone() if cd_disp else (0,)
        recibido = int(row_rec[0] or 0)
        row_cd = conn.execute(
            "SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM cd_despachos"
        ).fetchone()
        enviados_cd = int((row_cd[0] or 0) if row_cd else 0)
        devueltos = int((row_cd[1] or 0) if row_cd else 0)
        row_ori = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM cd_envios_origen WHERE id <> ?",
            (envio_id,)
        ).fetchone()
        enviados_origen_otros = int((row_ori[0] or 0) if row_ori else 0)
        entradas_totales = recibido + devueltos
        stock_excl = entradas_totales - enviados_cd - enviados_origen_otros
        if nuevas_cajas > stock_excl:
            conn.execute("ROLLBACK"); return False, f"Stock insuficiente. Disponible: {int(stock_excl)}"
        conn.execute(
            "UPDATE cd_envios_origen SET fecha = ?, cajas_enviadas = ? WHERE id = ?",
            (nueva_fecha, nuevas_cajas, envio_id)
        )
        conn.commit(); return True, "Envío actualizado"
    except Exception as e:
        try: conn.execute("ROLLBACK")
        except Exception: pass
        return False, f"Error al actualizar: {e}"
    finally:
        conn.close()

def cd_eliminar_envio_origen(envio_id):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM cd_envios_origen WHERE id = ?", (envio_id,))
        conn.commit(); return True, "Envío eliminado"
    except Exception as e:
        return False, f"Error al eliminar: {e}"
    finally:
        conn.close()

# =====================
# DESPACHOS CD
# =====================

def cd_crear_despacho(cd_local, destino_local, fecha, cajas_enviadas):
    cajas_enviadas = int(cajas_enviadas)
    if cajas_enviadas <= 0:
        return False, "Cantidad inválida"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Calcular stock disponible igual que en cd_totales
        cd_disp = _get_cd_display()
        recibido_viajes = 0
        if cd_disp:
            row_r = conn.execute(
                "SELECT SUM(cajas_enviadas) FROM viaje_locales WHERE numero_local = ?",
                (cd_disp,)
            ).fetchone()
            recibido_viajes = int((row_r[0] or 0) if row_r else 0)
        row_cd = conn.execute(
            "SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM cd_despachos"
        ).fetchone()
        enviados_cd_total = int((row_cd[0] or 0) if row_cd else 0)
        devueltos_total = int((row_cd[1] or 0) if row_cd else 0)
        row_ori = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM cd_envios_origen"
        ).fetchone()
        enviados_origen = int((row_ori[0] or 0) if row_ori else 0)
        stock_disponible = (recibido_viajes + devueltos_total) - enviados_cd_total - enviados_origen
        if cajas_enviadas > stock_disponible:
            conn.execute("ROLLBACK")
            return False, f"Stock insuficiente. Disponible: {stock_disponible}"
        conn.execute(
            "INSERT INTO cd_despachos (cd_local, destino_local, fecha, cajas_enviadas) VALUES (?, ?, ?, ?)",
            (cd_local, destino_local, fecha, cajas_enviadas)
        )
        conn.commit(); return True, "Despacho registrado"
    except Exception as e:
        try: conn.execute("ROLLBACK")
        except Exception: pass
        return False, f"Error: {e}"
    finally:
        conn.close()

def cd_listar_despachos(start_date=None, end_date=None, cd_local=None):
    conn = get_connection()
    try:
        query = "SELECT * FROM cd_despachos WHERE 1=1"
        params = []
        if start_date:
            query += " AND date(fecha) >= date(?)"; params.append(start_date)
        if end_date:
            query += " AND date(fecha) <= date(?)"; params.append(end_date)
        if cd_local and cd_local != "Todos":
            query += " AND cd_local = ?"; params.append(cd_local)
        query += " ORDER BY fecha DESC, id DESC"
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

def cd_registrar_devolucion(despacho_id, cantidad):
    cantidad = int(cantidad)
    if cantidad <= 0:
        return False, "Cantidad inválida"
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT cajas_enviadas, cajas_devueltas FROM cd_despachos WHERE id = ?",
            (despacho_id,)
        ).fetchone()
        if not row:
            return False, "Despacho no encontrado"
        envi, dev = row
        pend = (envi or 0) - (dev or 0)
        if cantidad > pend:
            return False, "Excede pendientes"
        conn.execute(
            "UPDATE cd_despachos SET cajas_devueltas = cajas_devueltas + ? WHERE id = ?",
            (cantidad, despacho_id)
        )
        conn.commit(); return True, "Devolución registrada"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()

def cd_actualizar_despacho(despacho_id, nueva_fecha, nuevas_cajas):
    nuevas_cajas = int(nuevas_cajas)
    if nuevas_cajas <= 0:
        return False, "La cantidad enviada debe ser mayor a 0"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row_cur = conn.execute(
            "SELECT cajas_enviadas FROM cd_despachos WHERE id = ?",
            (despacho_id,)
        ).fetchone()
        if not row_cur:
            conn.execute("ROLLBACK"); return False, "Despacho no encontrado"
        old_enviadas = int(row_cur[0] or 0)
        cd_disp = _get_cd_display()
        row_rec = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM viaje_locales WHERE numero_local = ?",
            (cd_disp,)
        ).fetchone() if cd_disp else (0,)
        recibido = int(row_rec[0] or 0)
        row_cd = conn.execute(
            "SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM cd_despachos"
        ).fetchone()
        enviados_cd_total = int((row_cd[0] or 0) if row_cd else 0)
        devueltos_total = int((row_cd[1] or 0) if row_cd else 0)
        row_ori = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM cd_envios_origen"
        ).fetchone()
        enviados_origen = int((row_ori[0] or 0) if row_ori else 0)
        stock_actual = (recibido + devueltos_total) - enviados_cd_total - enviados_origen
        if stock_actual + old_enviadas - nuevas_cajas < 0:
            conn.execute("ROLLBACK"); return False, f"Stock insuficiente para {nuevas_cajas}. Disponible: {stock_actual + old_enviadas}"
        conn.execute(
            "UPDATE cd_despachos SET fecha = ?, cajas_enviadas = ? WHERE id = ?",
            (nueva_fecha, nuevas_cajas, despacho_id)
        )
        conn.commit(); return True, "Despacho actualizado"
    except Exception as e:
        try: conn.execute("ROLLBACK")
        except Exception: pass
        return False, f"Error al actualizar: {e}"
    finally:
        conn.close()

def cd_actualizar_despacho_detallado(despacho_id, nueva_fecha, nuevas_enviadas, nuevas_devueltas):
    nuevas_enviadas = int(nuevas_enviadas)
    nuevas_devueltas = int(nuevas_devueltas)
    if nuevas_enviadas <= 0:
        return False, "La cantidad enviada debe ser mayor a 0"
    if nuevas_devueltas < 0:
        return False, "La cantidad devuelta no puede ser negativa"
    if nuevas_devueltas > nuevas_enviadas:
        return False, "Las devueltas no pueden superar a las enviadas"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT cajas_enviadas, cajas_devueltas FROM cd_despachos WHERE id = ?",
            (despacho_id,)
        ).fetchone()
        if not row:
            conn.execute("ROLLBACK"); return False, "Despacho no encontrado"
        old_enviadas = int(row[0] or 0)
        cd_disp = _get_cd_display()
        row_rec = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM viaje_locales WHERE numero_local = ?",
            (cd_disp,)
        ).fetchone() if cd_disp else (0,)
        recibido = int(row_rec[0] or 0)
        row_cd = conn.execute(
            "SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM cd_despachos"
        ).fetchone()
        enviados_cd_total = int((row_cd[0] or 0) if row_cd else 0)
        devueltos_total = int((row_cd[1] or 0) if row_cd else 0)
        row_ori = conn.execute(
            "SELECT SUM(cajas_enviadas) FROM cd_envios_origen"
        ).fetchone()
        enviados_origen = int((row_ori[0] or 0) if row_ori else 0)
        stock_actual = (recibido + devueltos_total) - enviados_cd_total - enviados_origen
        if stock_actual + old_enviadas - nuevas_enviadas < 0:
            conn.execute("ROLLBACK"); return False, f"Stock insuficiente para aumentar a {nuevas_enviadas}. Disponible: {stock_actual + old_enviadas}"
        conn.execute(
            "UPDATE cd_despachos SET fecha = ?, cajas_enviadas = ?, cajas_devueltas = ? WHERE id = ?",
            (nueva_fecha, nuevas_enviadas, nuevas_devueltas, despacho_id)
        )
        conn.commit(); return True, "Despacho actualizado"
    except Exception as e:
        try: conn.execute("ROLLBACK")
        except Exception: pass
        return False, f"Error al actualizar: {e}"
    finally:
        conn.close()

def cd_eliminar_despacho(despacho_id):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT cajas_devueltas FROM cd_despachos WHERE id = ?",
            (despacho_id,)
        ).fetchone()
        if not row:
            return False, "Despacho no encontrado"
        if (row[0] or 0) > 0:
            return False, "No se puede eliminar: ya tiene devoluciones registradas"
        conn.execute("DELETE FROM cd_despachos WHERE id = ?", (despacho_id,))
        conn.commit(); return True, "Despacho eliminado"
    except Exception as e:
        return False, f"Error al eliminar: {e}"
    finally:
        conn.close()

def cd_eliminar_despacho_forzado(despacho_id):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM cd_despachos WHERE id = ?", (despacho_id,))
        conn.commit(); return True, "Despacho eliminado (forzado)"
    except Exception as e:
        return False, f"Error al eliminar: {e}"
    finally:
        conn.close()

def cd_revertir_despacho_a_pendiente(despacho_id):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT cajas_enviadas, cajas_devueltas FROM cd_despachos WHERE id = ?",
            (despacho_id,)
        ).fetchone()
        if not row:
            return False, "Despacho no encontrado"
        cajas_devueltas = int(row[1] or 0)
        conn.execute(
            "UPDATE cd_despachos SET cajas_devueltas = 0 WHERE id = ?",
            (despacho_id,)
        )
        conn.commit(); return True, f"Despacho revertido a pendiente. {cajas_devueltas} cajas marcadas como no devueltas."
    except Exception as e:
        return False, f"Error al revertir: {e}"
    finally:
        conn.close()

def cd_pendientes_por_destino(start_date=None, end_date=None):
    conn = get_connection()
    try:
        query = "SELECT destino_local, SUM(cajas_enviadas) AS enviadas, SUM(cajas_devueltas) AS devueltas FROM cd_despachos WHERE 1=1"
        params = []
        if start_date:
            query += " AND date(fecha) >= date(?)"; params.append(start_date)
        if end_date:
            query += " AND date(fecha) <= date(?)"; params.append(end_date)
        query += " GROUP BY destino_local"
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()
    if df.empty:
        return df
    df["enviadas"] = df["enviadas"].fillna(0).astype(int)
    df["devueltas"] = df["devueltas"].fillna(0).astype(int)
    df["pendientes"] = df["enviadas"] - df["devueltas"]
    df = df[df["pendientes"] > 0].sort_values("pendientes", ascending=False)
    return df

def cd_registrar_devolucion_por_destino(destino_display, cantidad):
    restante = int(cantidad)
    if restante <= 0:
        return 0
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, cajas_enviadas, cajas_devueltas
            FROM cd_despachos
            WHERE destino_local = ? AND cajas_devueltas < cajas_enviadas
            ORDER BY date(fecha) ASC, id ASC
            """,
            (destino_display,)
        ).fetchall()
        total_aplicado = 0
        for rid, envi, dev in rows:
            if restante <= 0:
                break
            pend = int(envi) - int(dev)
            aplicar = min(pend, restante)
            if aplicar > 0:
                conn.execute(
                    "UPDATE cd_despachos SET cajas_devueltas = cajas_devueltas + ? WHERE id = ?",
                    (aplicar, rid)
                )
                restante -= aplicar
                total_aplicado += aplicar
        conn.commit(); return total_aplicado
    finally:
        conn.close()

def cd_registrar_devolucion_todas_por_destino(destino_display):
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE cd_despachos SET cajas_devueltas = cajas_enviadas WHERE destino_local = ? AND cajas_devueltas < cajas_enviadas",
            (destino_display,)
        )
        conn.commit()
    finally:
        conn.close()
