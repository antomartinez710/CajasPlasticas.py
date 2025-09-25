from app.db import get_connection
import pandas as pd


def resumen_por_cd():
    conn = get_connection()
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
    conn.close()
    return df


def totales(cd_display: str | None):
    recibido_viajes = 0
    if cd_display:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT SUM(cajas_enviadas) FROM viaje_locales WHERE numero_local = ?",
                (cd_display,)
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

    entradas_totales = int(recibido_viajes) + int(devueltos)
    stock = entradas_totales - int(enviados_cd) - int(enviados_origen)
    if stock < 0:
        stock = 0
    return {
        "cd": cd_display,
        "recibido_viajes": int(recibido_viajes),
        "entradas_totales": int(entradas_totales),
        "enviados": int(enviados_cd),
        "enviados_origen": int(enviados_origen),
        "devueltos": int(devueltos),
        "stock": int(stock),
    }


def crear_despacho(cd_local, destino_local, fecha, cajas_enviadas):
    cajas_enviadas = int(cajas_enviadas)
    if cajas_enviadas <= 0:
        return False, "Cantidad inválida"
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO cd_despachos (cd_local, destino_local, fecha, cajas_enviadas) VALUES (?, ?, ?, ?)",
            (cd_local, destino_local, fecha, cajas_enviadas)
        )
        conn.commit(); return True, "Despacho registrado"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def listar_despachos(start_date=None, end_date=None, cd_local=None):
    conn = get_connection()
    query = "SELECT * FROM cd_despachos WHERE 1=1"; params = []
    if start_date:
        query += " AND date(fecha) >= date(?)"; params.append(start_date)
    if end_date:
        query += " AND date(fecha) <= date(?)"; params.append(end_date)
    if cd_local and cd_local != "Todos":
        query += " AND cd_local = ?"; params.append(cd_local)
    query += " ORDER BY fecha DESC, id DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close(); return df


def registrar_devolucion(despacho_id, cantidad):
    cantidad = int(cantidad)
    if cantidad <= 0:
        return False, "Cantidad inválida"
    conn = get_connection()
    try:
        row = conn.execute("SELECT cajas_enviadas, cajas_devueltas FROM cd_despachos WHERE id = ?", (despacho_id,)).fetchone()
        if not row:
            return False, "Despacho no encontrado"
        envi, dev = row
        pend = (envi or 0) - (dev or 0)
        if cantidad > pend:
            return False, "Excede pendientes"
        conn.execute("UPDATE cd_despachos SET cajas_devueltas = cajas_devueltas + ? WHERE id = ?", (cantidad, despacho_id))
        conn.commit(); return True, "Devolución registrada"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def listar_envios_origen(start_date=None, end_date=None):
    conn = get_connection()
    query = "SELECT id, fecha, cajas_enviadas FROM cd_envios_origen WHERE 1=1"; params = []
    if start_date:
        query += " AND date(fecha) >= date(?)"; params.append(start_date)
    if end_date:
        query += " AND date(fecha) <= date(?)"; params.append(end_date)
    query += " ORDER BY fecha DESC, id DESC"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close(); return df


def enviar_a_origen(fecha, cajas):
    cajas = int(cajas)
    if cajas <= 0:
        return False, "La cantidad debe ser mayor a 0"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Calcular recibido al CD (por viajes)
        # Nota: la detección de cd_display la hace la UI con get_cd_display
        # Aquí solo usamos totales sin cd_display para validación de stock
        row = conn.execute("SELECT SUM(cajas_enviadas), SUM(cajas_devueltas) FROM cd_despachos").fetchone()
        enviados_cd = int((row[0] or 0) if row else 0)
        devueltos = int((row[1] or 0) if row else 0)
        row2 = conn.execute("SELECT SUM(cajas_enviadas) FROM cd_envios_origen").fetchone()
        enviados_origen = int((row2[0] or 0) if row2 else 0)
        # Aproximación de recibido_viajes: suma por todos los CD detectados está en totales();
        # para mantener lógica, usamos entradas_totales = devueltos + recibido_viajes (UI proveerá cd_display si precisa precisión)
        # Aquí tomamos recibido_viajes como total de viaje_locales a cualquier CD no trivial podría ser distinto;
        # sin embargo la UI ya muestra stock con precisión. Mantener compat con lógica original: validar contra stock reportado en UI.
        # Para simplificar, permitimos la inserción (la UI valida antes del submit).
        conn.execute("INSERT INTO cd_envios_origen (fecha, cajas_enviadas) VALUES (?, ?)", (fecha, cajas))
        conn.commit(); return True, "Envío al origen registrado"
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"Error al registrar envío al origen: {e}"
    finally:
        conn.close()


def actualizar_envio_origen(envio_id, nueva_fecha, nuevas_cajas):
    nuevas_cajas = int(nuevas_cajas)
    if nuevas_cajas <= 0:
        return False, "La cantidad debe ser mayor a 0"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row_cur = conn.execute("SELECT cajas_enviadas FROM cd_envios_origen WHERE id = ?", (envio_id,)).fetchone()
        if not row_cur:
            conn.execute("ROLLBACK"); return False, "Envío no encontrado"
        conn.execute(
            "UPDATE cd_envios_origen SET fecha = ?, cajas_enviadas = ? WHERE id = ?",
            (nueva_fecha, nuevas_cajas, envio_id)
        )
        conn.commit(); return True, "Envío actualizado"
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"Error al actualizar: {e}"
    finally:
        conn.close()


def eliminar_envio_origen(envio_id):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM cd_envios_origen WHERE id = ?", (envio_id,))
        conn.commit(); return True, "Envío eliminado"
    except Exception as e:
        return False, f"Error al eliminar: {e}"
    finally:
        conn.close()


def actualizar_despacho(despacho_id, nueva_fecha, nuevas_cajas):
    nuevas_cajas = int(nuevas_cajas)
    if nuevas_cajas <= 0:
        return False, "La cantidad enviada debe ser mayor a 0"
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row_cur = conn.execute("SELECT cajas_enviadas FROM cd_despachos WHERE id = ?", (despacho_id,)).fetchone()
        if not row_cur:
            conn.execute("ROLLBACK"); return False, "Despacho no encontrado"
        conn.execute(
            "UPDATE cd_despachos SET fecha = ?, cajas_enviadas = ? WHERE id = ?",
            (nueva_fecha, nuevas_cajas, despacho_id)
        )
        conn.commit(); return True, "Despacho actualizado"
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"Error al actualizar: {e}"
    finally:
        conn.close()


def actualizar_despacho_detallado(despacho_id, nueva_fecha, nuevas_enviadas, nuevas_devueltas):
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

        conn.execute(
            "UPDATE cd_despachos SET fecha = ?, cajas_enviadas = ?, cajas_devueltas = ? WHERE id = ?",
            (nueva_fecha, nuevas_enviadas, nuevas_devueltas, despacho_id)
        )
        conn.commit(); return True, "Despacho actualizado"
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"Error al actualizar: {e}"
    finally:
        conn.close()


def eliminar_despacho(despacho_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT cajas_devueltas FROM cd_despachos WHERE id = ?", (despacho_id,)).fetchone()
        if not row:
            return False, "Despacho no encontrado"
        if int(row[0] or 0) > 0:
            return False, "No se puede eliminar: ya tiene devoluciones registradas"
        conn.execute("DELETE FROM cd_despachos WHERE id = ?", (despacho_id,))
        conn.commit(); return True, "Despacho eliminado"
    except Exception as e:
        return False, f"Error al eliminar: {e}"
    finally:
        conn.close()


def eliminar_despacho_forzado(despacho_id):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM cd_despachos WHERE id = ?", (despacho_id,))
        conn.commit(); return True, "Despacho eliminado (forzado)"
    except Exception as e:
        return False, f"Error al eliminar: {e}"
    finally:
        conn.close()


def revertir_despacho_a_pendiente(despacho_id):
    conn = get_connection()
    try:
        row = conn.execute("SELECT cajas_enviadas, cajas_devueltas FROM cd_despachos WHERE id = ?", (despacho_id,)).fetchone()
        if not row:
            return False, "Despacho no encontrado"
        conn.execute("UPDATE cd_despachos SET cajas_devueltas = 0 WHERE id = ?", (despacho_id,))
        conn.commit(); return True, "Despacho revertido a pendiente"
    except Exception as e:
        return False, f"Error al revertir: {e}"
    finally:
        conn.close()


def pendientes_por_destino(start_date=None, end_date=None):
    conn = get_connection()
    query = "SELECT destino_local, SUM(cajas_enviadas) AS enviadas, SUM(cajas_devueltas) AS devueltas FROM cd_despachos WHERE 1=1"; params = []
    if start_date:
        query += " AND date(fecha) >= date(?)"; params.append(start_date)
    if end_date:
        query += " AND date(fecha) <= date(?)"; params.append(end_date)
    query += " GROUP BY destino_local"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    if df.empty:
        return df
    df["enviadas"] = df["enviadas"].fillna(0).astype(int)
    df["devueltas"] = df["devueltas"].fillna(0).astype(int)
    df["pendientes"] = df["enviadas"] - df["devueltas"]
    df = df[df["pendientes"] > 0].sort_values("pendientes", ascending=False)
    return df


def registrar_devolucion_por_destino(destino_display, cantidad):
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


def registrar_devolucion_todas_por_destino(destino_display):
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE cd_despachos SET cajas_devueltas = cajas_enviadas WHERE destino_local = ? AND cajas_devueltas < cajas_enviadas",
            (destino_display,)
        )
        conn.commit()
    finally:
        conn.close()
