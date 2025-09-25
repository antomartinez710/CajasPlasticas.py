from app.db import get_connection
import pandas as pd
import sqlite3
from typing import Optional, Tuple


def get_viajes_detallados(fecha_desde=None, fecha_hasta=None, chofer_id=None, estado=None) -> pd.DataFrame:
    conn = get_connection()
    query = [
        "SELECT v.id, v.fecha_viaje, v.estado, c.nombre AS chofer,",
        "       COALESCE(COUNT(vl.id),0) AS total_locales,",
        "       COALESCE(SUM(vl.cajas_enviadas),0) AS total_enviadas,",
        "       COALESCE(SUM(vl.cajas_devueltas),0) AS total_devueltas,",
        "       COALESCE(SUM(vl.cajas_enviadas),0) - COALESCE(SUM(vl.cajas_devueltas),0) AS pendientes",
        "FROM viajes v",
        "JOIN choferes c ON v.chofer_id = c.id",
        "LEFT JOIN viaje_locales vl ON v.id = vl.viaje_id"
    ]
    filtros = []; params = []
    if fecha_desde:
        filtros.append("v.fecha_viaje >= ?"); params.append(fecha_desde)
    if fecha_hasta:
        filtros.append("v.fecha_viaje <= ?"); params.append(fecha_hasta)
    if chofer_id:
        filtros.append("v.chofer_id = ?"); params.append(chofer_id)
    if estado and estado != "Todos":
        filtros.append("v.estado = ?"); params.append(estado)
    if filtros:
        query.append("WHERE " + " AND ".join(filtros))
    query.append("GROUP BY v.id ORDER BY v.fecha_viaje DESC, v.id DESC")
    try:
        df = pd.read_sql_query(" ".join(query), conn, params=params)
    finally:
        conn.close()
    return df


def get_viaje_locales(viaje_id: int) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT id, numero_local, cajas_enviadas, cajas_devueltas, (cajas_enviadas - cajas_devueltas) AS pendientes FROM viaje_locales WHERE viaje_id = ? ORDER BY id",
            conn,
            params=(viaje_id,)
        )
    finally:
        conn.close()
    return df


def _log_devolucion(viaje_id: int, viaje_local_id: int, numero_local: str, cantidad: int, tipo: str = "individual", usuario: Optional[str] = None):
    """Inserta una fila en devoluciones_log (best effort)."""
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute(
            "INSERT INTO devoluciones_log (viaje_id, viaje_local_id, numero_local, cantidad, tipo, usuario) VALUES (?,?,?,?,?,?)",
            (viaje_id, viaje_local_id, numero_local, int(cantidad), tipo, usuario)
        )
        conn.commit(); conn.close()
    except Exception:
        # logging silencioso para no romper el flujo principal
        try:
            conn.close()
        except Exception:
            pass


def registrar_devolucion(viaje_local_id: int, cantidad: int, usuario: Optional[str] = None):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT viaje_id, numero_local, cajas_enviadas, cajas_devueltas FROM viaje_locales WHERE id = ?", (viaje_local_id,))
        row = cur.fetchone()
        if not row:
            return False, "Registro no encontrado"
        viaje_id, numero_local, enviadas, devueltas = row
        pendientes = int(enviadas or 0) - int(devueltas or 0)
        cantidad = int(cantidad)
        if cantidad <= 0:
            return False, "Cantidad inválida"
        if cantidad > pendientes:
            return False, "Cantidad excede pendientes"
        cur.execute("UPDATE viaje_locales SET cajas_devueltas = cajas_devueltas + ? WHERE id = ?", (cantidad, viaje_local_id))
        conn.commit()
        _log_devolucion(viaje_id, viaje_local_id, numero_local, cantidad, "individual", usuario)
        return True, "Devolución registrada"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def registrar_devolucion_todas_por_viaje(viaje_id: int, usuario: Optional[str] = None):
    """Marca como devueltas todas las cajas pendientes del viaje, registrando una entrada por cada local con pendientes."""
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT id, numero_local, cajas_enviadas, cajas_devueltas FROM viaje_locales WHERE viaje_id = ?", (viaje_id,))
        rows = cur.fetchall()
        if not rows:
            return False, "Viaje sin locales"
        total_registros = 0
        for (vl_id, numero_local, enviadas, devueltas) in rows:
            enviadas = int(enviadas or 0); devueltas = int(devueltas or 0)
            pendientes = enviadas - devueltas
            if pendientes > 0:
                cur.execute("UPDATE viaje_locales SET cajas_devueltas = cajas_enviadas WHERE id = ?", (vl_id,))
                total_registros += pendientes
                _log_devolucion(viaje_id, vl_id, numero_local, pendientes, "masiva", usuario)
        conn.commit()
        return True, f"Devoluciones completadas ({total_registros} cajas)"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def listar_devoluciones_log(viaje_id: Optional[int] = None, fecha_desde: Optional[str] = None, fecha_hasta: Optional[str] = None, numero_local: Optional[str] = None):
    """Devuelve un DataFrame con el historial de devoluciones (más reciente primero)."""
    conn = get_connection()
    try:
        base = [
            "SELECT dl.id, dl.created_at, dl.viaje_id, dl.viaje_local_id, dl.numero_local, dl.cantidad, dl.tipo, dl.usuario,",
            "       vl.cajas_enviadas, vl.cajas_devueltas, (vl.cajas_enviadas - vl.cajas_devueltas) AS pendientes_actuales",
            "FROM devoluciones_log dl",
            "JOIN viaje_locales vl ON dl.viaje_local_id = vl.id"
        ]
        filtros = []; params = []
        if viaje_id:
            filtros.append("dl.viaje_id = ?"); params.append(viaje_id)
        if fecha_desde:
            filtros.append("date(dl.created_at) >= ?"); params.append(fecha_desde)
        if fecha_hasta:
            filtros.append("date(dl.created_at) <= ?"); params.append(fecha_hasta)
        if numero_local:
            filtros.append("dl.numero_local = ?"); params.append(numero_local)
        if filtros:
            base.append("WHERE " + " AND ".join(filtros))
        base.append("ORDER BY dl.id DESC")
        df = pd.read_sql_query(" ".join(base), conn, params=params)
        return df
    finally:
        conn.close()


def actualizar_devolucion_log(entry_id: int, nueva_cantidad: int) -> Tuple[bool, str]:
    """Permite editar la cantidad de una devolución ya registrada ajustando viaje_locales en consecuencia.
    No permite dejar cantidades <=0; para eliminar usar borrar.
    """
    nueva_cantidad = int(nueva_cantidad)
    if nueva_cantidad <= 0:
        return False, "Cantidad inválida"
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT viaje_local_id, cantidad FROM devoluciones_log WHERE id = ?", (entry_id,))
        row = cur.fetchone()
        if not row:
            return False, "Registro no encontrado"
        viaje_local_id, cantidad_original = row
        delta = nueva_cantidad - int(cantidad_original)
        if delta == 0:
            return True, "Sin cambios"
        # Validar límites
        cur.execute("SELECT cajas_enviadas, cajas_devueltas FROM viaje_locales WHERE id = ?", (viaje_local_id,))
        vl = cur.fetchone()
        if not vl:
            return False, "viaje_local no existe"
        enviadas, devueltas = map(int, vl)
        nuevas_devueltas = devueltas + delta
        if nuevas_devueltas < 0 or nuevas_devueltas > enviadas:
            return False, "Ajuste excede límites"
        cur.execute("UPDATE viaje_locales SET cajas_devueltas = ? WHERE id = ?", (nuevas_devueltas, viaje_local_id))
        cur.execute("UPDATE devoluciones_log SET cantidad = ? WHERE id = ?", (nueva_cantidad, entry_id))
        conn.commit(); return True, "Actualizado"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def eliminar_devolucion_log(entry_id: int) -> Tuple[bool, str]:
    """Elimina una devolución revertiendo su efecto en viaje_locales."""
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("SELECT viaje_local_id, cantidad FROM devoluciones_log WHERE id = ?", (entry_id,))
        row = cur.fetchone()
        if not row:
            return False, "Registro no encontrado"
        viaje_local_id, cantidad = row
        # revertir
        cur.execute("SELECT cajas_enviadas, cajas_devueltas FROM viaje_locales WHERE id = ?", (viaje_local_id,))
        vl = cur.fetchone()
        if not vl:
            return False, "viaje_local no existe"
        enviadas, devueltas = map(int, vl)
        nuevas = devueltas - int(cantidad)
        if nuevas < 0:
            return False, "Inconsistencia: no se puede revertir"
        cur.execute("UPDATE viaje_locales SET cajas_devueltas = ? WHERE id = ?", (nuevas, viaje_local_id))
        cur.execute("DELETE FROM devoluciones_log WHERE id = ?", (entry_id,))
        conn.commit(); return True, "Eliminado"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def eliminar_viaje(viaje_id: int):
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM viaje_locales WHERE viaje_id = ?", (viaje_id,))
        cur.execute("DELETE FROM viajes WHERE id = ?", (viaje_id,))
        conn.commit()
        return True, "Viaje eliminado"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        conn.close()


def viaje_agregar_local(viaje_id: int, numero_local_display: str, cajas_enviadas: int):
    cajas_enviadas = int(cajas_enviadas)
    if cajas_enviadas <= 0:
        return False, "Cantidad inválida"
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("SELECT 1 FROM viaje_locales WHERE viaje_id=? AND numero_local=?", (viaje_id, numero_local_display))
        if cur.fetchone():
            conn.close(); return False, "El local ya está en el viaje"
        cur.execute(
            "INSERT INTO viaje_locales (viaje_id, numero_local, cajas_enviadas, cajas_devueltas) VALUES (?,?,?,0)",
            (viaje_id, numero_local_display, cajas_enviadas)
        )
        conn.commit(); conn.close(); return True, "Local agregado"
    except Exception as e:
        return False, f"Error: {e}"


def viaje_eliminar_local(item_id: int):
    try:
        conn = get_connection(); cur = conn.cursor()
        row = cur.execute("SELECT cajas_devueltas FROM viaje_locales WHERE id=?", (item_id,)).fetchone()
        if not row:
            conn.close(); return False, "Registro no existe"
        if int(row[0] or 0) > 0:
            conn.close(); return False, "No se puede eliminar: ya tiene devoluciones"
        cur.execute("DELETE FROM viaje_locales WHERE id=?", (item_id,))
        conn.commit(); conn.close(); return True, "Local eliminado"
    except Exception as e:
        return False, f"Error: {e}"


def crear_viaje_con_locales(fecha_viaje, chofer_id: int, items: list):
    """Crea un viaje y sus locales asociados de forma transaccional.
    items: lista de dicts {display, cajas}
    """
    if not items:
        return False, "Debes agregar al menos un local"
    try:
        conn = get_connection(); cur = conn.cursor()
        conn.execute("BEGIN")
        cur.execute("INSERT INTO viajes (fecha_viaje, chofer_id, estado) VALUES (?,?,?)", (fecha_viaje, chofer_id, 'En Curso'))
        viaje_id = cur.lastrowid
        for it in items:
            disp = it['display']
            cajas = int(it['cajas'])
            if cajas <= 0:
                conn.execute("ROLLBACK"); conn.close(); return False, f"Cantidad inválida para {disp}"
            cur.execute(
                "INSERT INTO viaje_locales (viaje_id, numero_local, cajas_enviadas, cajas_devueltas) VALUES (?,?,?,0)",
                (viaje_id, disp, cajas)
            )
        conn.commit(); conn.close(); return True, f"Viaje #{viaje_id} creado"
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"Error creando viaje: {e}"


def crear_viaje(chofer_id: int, fecha_viaje, locales: list):
    """Crea un viaje insertando primero viaje y luego cada local.
    locales: lista de dicts {'numero_local': str, 'cajas_enviadas': int}
    (Se mantiene por compatibilidad con la interfaz antigua)."""
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


def actualizar_estado_viaje(viaje_id: int, nuevo_estado: str):
    conn = get_connection()
    try:
        conn.execute("UPDATE viajes SET estado = ? WHERE id = ?", (nuevo_estado, viaje_id))
        conn.commit(); return True
    finally:
        conn.close()
