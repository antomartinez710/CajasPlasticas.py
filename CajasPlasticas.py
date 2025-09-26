"""Aplicación Streamlit Control de Cajas.

Arquitectura (resumen rápido):
--------------------------------
Se migró gradualmente la lógica del monolito original a una capa de servicios:

Servicios activos:
    app/services/stats_service.py   -> Métricas y datos para dashboard (pendientes, totales, gráficos)
    app/services/viajes_service.py  -> Viajes: creación, listado, estado, devoluciones + historial (devoluciones_log)
    app/services/cd_service.py      -> Centro de Distribución: despachos, devoluciones CD, envíos a origen (Pastas Frescas), stock

Reglas de uso:
    - Toda la UI debe llamar únicamente a funciones prefijadas con svc_* importadas arriba.
    - No se debe invocar directamente funciones de modelos dentro de la UI (aislamiento capa de datos).
    - Los wrappers mdl_* subsisten temporalmente solo para locales / choferes / users hasta su extracción (Fase 4).

Roles y permisos:
    admin        -> Acceso completo y acciones destructivas (eliminar, limpiar datos, forzar eliminaciones).
    cd_only      -> Puede operar en pestaña Centro de Distribución (crear / editar) pero no cambiar usuarios.
    no_cd_edit   -> Puede ver CD pero no crear / editar (solo lectura en secciones CD, PF, despachos).

Próximas fases sugeridas:
    Fase 4: extraer locales, choferes y usuarios a services/ (locales_service, users_service, choferes_service).
    Fase 5: agregar índices SQLite (viajes.fecha_viaje, devoluciones_log.created_at, cd_despachos.fecha, cd_envios_origen.fecha).
    Fase 6: caching selectivo con st.cache_data para catálogos (locales, choferes) y resúmenes agregados.
    Fase 7: logging estructurado (actions + usuario + timestamp) y tests unitarios básicos para services.

Notas técnicas:
    - Mantener atomicidad: cada operación de escritura se encapsula en su función service (facilita tests).
    - Validaciones UI (ej: devueltas <= enviadas) se hacen antes de llamar al service y el service revalida.
    - Evitar lógica de negocio dentro de widgets Streamlit.
"""

import streamlit as st
import sqlite3, os, pandas as pd, datetime
from datetime import timedelta
import plotly.graph_objects as go  # necesario para charts del dashboard

# Servicios externos
from app.services.stats_service import get_dashboard_stats, get_pendientes_por_local
from app.services.viajes_service import (
    listar_viajes as svc_listar_viajes,
    viaje_locales as svc_viaje_locales,
    crear_viaje as svc_crear_viaje,
    registrar_devolucion as svc_registrar_devolucion,
    registrar_devolucion_todas_por_viaje as svc_registrar_devolucion_todas_por_viaje,
    eliminar_viaje as svc_eliminar_viaje,
    actualizar_estado_viaje as svc_actualizar_estado_viaje,
    update_devueltas_viaje_locales as svc_update_devueltas_viaje_locales,
)
from app.services.cd_service import (
    cd_totales as svc_cd_totales,
    cd_enviar_a_origen as svc_cd_enviar_a_origen,
    cd_listar_envios_origen as svc_cd_listar_envios_origen,
    cd_actualizar_envio_origen as svc_cd_actualizar_envio_origen,
    cd_eliminar_envio_origen as svc_cd_eliminar_envio_origen,
    cd_crear_despacho as svc_cd_crear_despacho,
    cd_listar_despachos as svc_cd_listar_despachos,
    cd_registrar_devolucion as svc_cd_registrar_devolucion,
    cd_actualizar_despacho_detallado as svc_cd_actualizar_despacho_detallado,
    cd_eliminar_despacho_forzado as svc_cd_eliminar_despacho_forzado,
    cd_revertir_despacho_a_pendiente as svc_cd_revertir_despacho_a_pendiente,
)
from app.services.locales_service import (
    listar_locales as svc_loc_listar_locales,
    crear_local as svc_loc_crear_local,
    actualizar_local as svc_loc_actualizar_local,
    eliminar_local as svc_loc_eliminar_local,
    siguiente_numero as svc_loc_siguiente_numero,
    get_catalogo_con_display as svc_loc_get_catalogo_con_display,
)
from app.services.choferes_service import (
    listar_choferes as svc_ch_listar_choferes,
    crear_chofer as svc_ch_crear_chofer,
)
from app.services.users_service import (
    crear_usuario as svc_user_crear_usuario,
    listar_usuarios as svc_user_listar_usuarios,
    actualizar_usuario as svc_user_actualizar_usuario,
    eliminar_usuario as svc_user_eliminar_usuario,
    set_password as svc_user_set_password,
    obtener_usuario as svc_user_obtener_usuario,
    verificar_password as svc_user_verificar_password,
    hash_password as svc_user_hash_password,
)

## Nota: todos los usos de viajes/devoluciones llaman directamente a las funciones svc_* importadas.

# -------------------------------
# CONSTANTES DE MARCA (restauradas / nuevas)
# -------------------------------


# Valores antiguos que otras partes podrían esperar
PAGE_TITLE = "Control de Cajas"
PAGE_ICON = "🚛"

try:
    st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")
except Exception:
    pass

# ---------------------------------
# IMPORTS MODELOS / FALLBACKS
# ---------------------------------
try:
    from app.db import get_connection as mdl_get_connection, init_database as mdl_init_db
except Exception:
    mdl_get_connection = mdl_init_db = None

try:
    from app.config import get_db_path as mdl_get_db_path
except Exception:
    mdl_get_db_path = None

## Import de modelos viajes eliminado: lógica consolidada en servicio viajes_service

## Import de modelos locales eliminado: ahora gestionado por app.services.locales_service

## Import de modelos choferes eliminado: ahora gestionado por app.services.choferes_service

## Import de modelos CD eliminado tras migración completa a cd_service



# Autenticación extendida (si existía) – mantener nombres previos
def auth_get_user(username: str):
    """Wrapper de compatibilidad: obtiene usuario vía servicio."""
    return svc_user_obtener_usuario(username)

def auth_verify_password(clear_pw: str, stored_hash: str):
    return svc_user_verificar_password(clear_pw, stored_hash)

# ---------------------------------
# WRAPPERS / FALLBACK FUNCTIONS
# ---------------------------------
def get_connection():
    if mdl_get_connection:
        try:
            return mdl_get_connection()
        except Exception:
            pass
    # fallback directo a archivo local original si existiera
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cajas_plasticas.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn

def init_database():
    if mdl_init_db:
        try:
            return mdl_init_db()
        except Exception:
            pass
    # fallback mínimo (solo users y viajes para evitar errores)
    conn = get_connection(); c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password_hash TEXT, role TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS choferes (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT UNIQUE, contacto TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS viajes (id INTEGER PRIMARY KEY AUTOINCREMENT, chofer_id INTEGER, fecha_viaje DATE, estado TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS viaje_locales (id INTEGER PRIMARY KEY AUTOINCREMENT, viaje_id INTEGER, numero_local TEXT, cajas_enviadas INTEGER, cajas_devueltas INTEGER DEFAULT 0)")
    c.execute("CREATE TABLE IF NOT EXISTS devoluciones_log (id INTEGER PRIMARY KEY AUTOINCREMENT, viaje_id INTEGER, viaje_local_id INTEGER, numero_local TEXT, cantidad INTEGER, tipo TEXT, usuario TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
    # Tablas CD básicas
    c.execute("CREATE TABLE IF NOT EXISTS cd_despachos (id INTEGER PRIMARY KEY AUTOINCREMENT, cd_local TEXT, destino_local TEXT, fecha DATE, cajas_enviadas INTEGER, cajas_devueltas INTEGER DEFAULT 0)")
    c.execute("CREATE TABLE IF NOT EXISTS cd_envios_origen (id INTEGER PRIMARY KEY AUTOINCREMENT, fecha DATE, cajas_enviadas INTEGER)")
    # Catálogo de locales si no existe
    c.execute("CREATE TABLE IF NOT EXISTS reception_local (id INTEGER PRIMARY KEY AUTOINCREMENT, numero INTEGER, nombre TEXT)")
    conn.commit(); conn.close()

init_database()

# Roles válidos
VALID_ROLES = ("admin", "cd_only", "no_cd_edit")

# ---------------------------------
# UTILIDADES / WRAPPERS ADICIONALES FALTANTES
# ---------------------------------

def get_db_path():
    if mdl_get_db_path:
        try:
            return mdl_get_db_path()
        except Exception:
            pass
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "cajas_plasticas.db")

def get_locales_catalogo():
    data = svc_loc_get_catalogo_con_display()
    if not data:
        return pd.DataFrame(columns=["id","numero","nombre","display"])
    import pandas as pd
    return pd.DataFrame(data, columns=["id","numero","nombre","display"])

## Funciones de viajes y devoluciones ahora provienen de app.services.viajes_service

## get_dashboard_stats y get_pendientes_por_local ahora provienen de app.services.stats_service

def hash_password(pw: str) -> str:  # compat
    return svc_user_hash_password(pw)

def auth_login(username: str, password: str):
    username = (username or "").strip()
    if not username or not password:
        return False, "Completa usuario y contraseña"
    user = auth_get_user(username)
    if not user:
        return False, "Usuario no encontrado"
    if not auth_verify_password(password, user.get("password_hash", "")):
        return False, "Contraseña incorrecta"
    st.session_state["user"] = {"id": user["id"], "username": user["username"], "role": user["role"]}
    return True, "Inicio de sesión exitoso"

def auth_logout():
    st.session_state.pop("user", None)

def set_user_password(user_id: int, new_password: str):
    return svc_user_set_password(user_id, new_password)

def create_user(username: str, password: str, role: str = "admin"):
    return svc_user_crear_usuario(username, password, role)

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

def list_users():
    return svc_user_listar_usuarios()

def update_user(user_id, new_username=None, new_role=None):
    return svc_user_actualizar_usuario(user_id, new_username=new_username, new_role=new_role)

def delete_user(user_id):
    return svc_user_eliminar_usuario(user_id)

# === Legacy style helpers (adaptados de versión anterior) ===
def crear_viaje(chofer_id, fecha_viaje, locales):
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


## Lógica CD movida a cd_service (cd_resumen_por_cd)

## Helpers de choferes migrados a app.services.choferes_service

def actualizar_estado_viaje(viaje_id, nuevo_estado):
    conn = get_connection()
    conn.execute("UPDATE viajes SET estado = ? WHERE id = ?", (nuevo_estado, viaje_id))
    conn.commit()
    conn.close()
    return True

def get_cd_display():
    """Intenta detectar el 'numero - nombre' del CD a partir del catálogo.
    Heurística: primer local cuyo nombre contiene 'CD' (case-insensitive).
    Devuelve None si no encuentra.
    """
    catalogo_df = get_locales_catalogo()
    if catalogo_df.empty:
        return None
    for _, row in catalogo_df.iterrows():
        nombre = str(row.get("nombre") or "")
        if nombre and ("cd" in nombre.lower()):
            numero = row.get("numero")
            # Devolver exactamente el mismo formato que se usa al guardar en viaje_locales (numero o "numero - nombre")
            # En creación de viaje usamos display "numero - nombre"; pero para matching de numero_local parece que se guarda sólo el display.
            # Intentamos ambas posibilidades, prefiriendo display completo.
            return f"{numero} - {nombre}" if nombre else str(numero)
    return None

## Lógica CD movida a cd_service (cd_totales)

## Lógica CD movida a cd_service (cd_enviar_a_origen)

## Lógica CD movida a cd_service (cd_actualizar_envio_origen)

## Lógica CD movida a cd_service (cd_eliminar_envio_origen)

## Lógica CD movida a cd_service (cd_actualizar_despacho)

## Lógica CD movida a cd_service (cd_actualizar_despacho_detallado)

## Lógica CD movida a cd_service (cd_eliminar_despacho)

## Lógica CD movida a cd_service (cd_eliminar_despacho_forzado)

## Lógica CD movida a cd_service (cd_revertir_despacho_a_pendiente)

## Lógica CD movida a cd_service (cd_pendientes_por_destino)

## Lógica CD movida a cd_service (cd_registrar_devolucion_por_destino)

## Lógica CD movida a cd_service (cd_registrar_devolucion_todas_por_destino)

# -------------------------------
# FUNCIONES CD BÁSICAS (restauradas)
# -------------------------------
## Lógica CD movida a cd_service (cd_crear_despacho)

## Lógica CD movida a cd_service (cd_listar_despachos)

## Lógica CD movida a cd_service (cd_registrar_devolucion)

## Lógica CD movida a cd_service (cd_listar_envios_origen)

# -------------------------------
# HEADER PERSONALIZADO
# -------------------------------
st.markdown("""
<div class="custom-header">
    <h1>🚛 Sistema de Control de Cajas</h1>
    <p>Gestión profesional de cajas plásticas por chofer</p>
</div>
""", unsafe_allow_html=True)

# -------------------------------
# SIDEBAR MEJORADA
# -------------------------------
with st.sidebar:
    # Bloque de marca en la barra lateral
    try:
        logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo.png")
    except Exception:
        logo_path = None
    col_logo, col_title = st.columns([1, 2])
    with col_logo:
        if logo_path and os.path.exists(logo_path):
            st.image(logo_path, width=96)
        else:
            st.markdown("<div style='width:96px;height:96px;border:1px solid #e5e7eb;border-radius:8px;display:flex;align-items:center;justify-content:center;color:#9ca3af;font-size:22px;'>PF</div>", unsafe_allow_html=True)
    with col_title:
    # Eliminado nombre de marca. Se muestran título y subtítulo.
        st.caption("Control de Cajas")
        st.caption("Pastas Frescas")

    st.markdown("---")
    # Login / User info
    user = st.session_state.get("user")
    if not user:
        st.markdown("### 🔐 Iniciar sesión")
        with st.form("login_form", clear_on_submit=False):
            u = st.text_input("Usuario", key="login_user")
            p = st.text_input("Contraseña", type="password", key="login_pass")
            do_login = st.form_submit_button("Ingresar", type="primary")
        if do_login:
            ok, msg = auth_login(u.strip(), p)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
        st.stop()
    else:
        role = user.get("role", "")
        st.markdown(f"👤 <strong>{user['username']}</strong>", unsafe_allow_html=True)
        role_badge = {
            "admin": "<span class='status-badge status-completado'>admin</span>",
            "cd_only": "<span class='status-badge status-activo'>cd_only</span>",
            "no_cd_edit": "<span class='status-badge status-pendiente'>no_cd_edit</span>",
        }.get(role, role)
        st.markdown(role_badge, unsafe_allow_html=True)
        with st.expander("Cambiar mi contraseña"):
            with st.form("change_my_password", clear_on_submit=True):
                cur = st.text_input("Contraseña actual", type="password")
                new1 = st.text_input("Nueva contraseña", type="password")
                new2 = st.text_input("Repetir nueva contraseña", type="password")
                do_change = st.form_submit_button("Actualizar contraseña")
            if do_change:
                if not cur or not new1 or not new2:
                    st.warning("Completa todos los campos")
                elif new1 != new2:
                    st.error("Las contraseñas no coinciden")
                else:
                    me = auth_get_user(user['username'])
                    if not me or not auth_verify_password(cur, me['password_hash']):
                        st.error("La contraseña actual no es correcta")
                    else:
                        ok, msg = set_user_password(me['id'], new1)
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg)
        if st.button("Cerrar sesión"):
            auth_logout()
            st.rerun()

    st.markdown("---")
    st.markdown("### 📋 Menú Principal")
    # Menú por rol
    all_items = ["🏠 Dashboard", "👷 Choferes", "🛣️ Viajes", "📥 Devoluciones", "🏪 Locales", "🏬 Centro de Distribución"]
    # Admin puede ver sección Usuarios
    if role == "admin":
        all_items.extend(["👥 Usuarios"])
    if role == "cd_only":
        items = ["🏬 Centro de Distribución"]
    else:
        items = all_items
    menu = st.selectbox(
        "",
        items,
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.markdown("### 📊 Resumen Rápido")
    stats = get_dashboard_stats()
    st.metric("Total Choferes", stats['total_choferes'])
    st.metric("Viajes Activos", stats['viajes_activos'])
    st.metric("Cajas Pendientes", stats['pendientes'])

# -------------------------------
# DASHBOARD
# -------------------------------

# -------------------------------
# LOCALES
# -------------------------------
if menu == "🏪 Locales":
    st.header("Gestión de Locales")

    with st.form("agregar_local"):
        numero = st.number_input("Número de local", min_value=1, step=1, value=svc_loc_siguiente_numero())
        nombre = st.text_input("Nombre del local")
        if st.form_submit_button("Agregar"):
            ok, msg = svc_loc_crear_local(int(numero), nombre)
            (st.success if ok else st.error)(msg)
            if ok:
                st.rerun()

    st.subheader("Locales cargados")
    data_locales = svc_loc_listar_locales()
    if data_locales:
        import pandas as pd
        df_locales = pd.DataFrame(data_locales).rename(columns={"id":"ID","numero":"Número","nombre":"Nombre"})

        # Filtros de búsqueda y orden
        with st.container():
            col_f1, col_f2 = st.columns([2, 1])
            with col_f1:
                filtro_texto = st.text_input("🔎 Buscar", placeholder="Por número o nombre…")
            with col_f2:
                ordenar_por = st.selectbox(
                    "Ordenar por",
                    ["Número ascendente", "Número descendente", "Nombre A-Z", "Nombre Z-A"],
                    index=0
                )

        # Aplicar filtro por texto
        df_filtrado = df_locales.copy()
        if filtro_texto:
            filtro = str(filtro_texto).strip()
            mask = (
                df_filtrado['Nombre'].astype(str).str.contains(filtro, case=False, na=False) |
                df_filtrado['Número'].astype(str).str.contains(filtro, case=False, na=False)
            )
            df_filtrado = df_filtrado[mask]

        # Aplicar ordenamiento
        if ordenar_por.startswith("Número"):
            asc = ordenar_por == "Número ascendente"
            df_filtrado = df_filtrado.sort_values(by='Número', ascending=asc)
        else:
            asc = ordenar_por == "Nombre A-Z"
            df_filtrado = df_filtrado.sort_values(by='Nombre', ascending=asc)

        # Métricas
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.metric("Total de locales registrados", len(df_locales))
        with col_m2:
            st.metric("Coincidencias con filtro", len(df_filtrado))

        # Mostrar tabla filtrada simple (sin cuadricula ni paginación)
        st.dataframe(df_filtrado, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("Acciones sobre locales")

        if df_filtrado.empty:
            st.info("No hay resultados con los filtros actuales. Ajusta la búsqueda para continuar.")
        else:
            # Selector para elegir el local a editar/eliminar (sobre el resultado filtrado)
            opciones_locales = [f"{row['Número']} - {row['Nombre']}" for _, row in df_filtrado.iterrows()]
            local_seleccionado = st.selectbox(
                "Selecciona un local para editar o eliminar:",
                opciones_locales,
                key="selector_local"
            )

            if local_seleccionado:
                # Encontrar el registro seleccionado dentro del filtrado
                indice_seleccionado = opciones_locales.index(local_seleccionado)
                fila_sel = df_filtrado.iloc[indice_seleccionado]
                id_local = int(fila_sel['ID'])
                numero_actual = int(fila_sel['Número'])
                nombre_actual = str(fila_sel['Nombre'])

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("##### ✏️ Editar Local")
                    with st.form(f"editar_local_{id_local}"):
                        nuevo_numero = st.number_input("Nuevo número:", min_value=1, value=numero_actual)
                        nuevo_nombre = st.text_input("Nuevo nombre:", value=nombre_actual)
                    
                        if st.form_submit_button("💾 Guardar cambios", use_container_width=True):
                            ok, msg = svc_loc_actualizar_local(id_local, int(nuevo_numero), nuevo_nombre)
                            (st.success if ok else st.error)(msg)
                            if ok:
                                st.rerun()

                with col2:
                    st.markdown("##### 🗑️ Eliminar Local")
                    st.warning(f"¿Estás seguro de que deseas eliminar el local **{numero_actual} - {nombre_actual}**?")
                    st.caption("⚠️ Esta acción no se puede deshacer.")

                    # Checkbox de confirmación
                    confirmar_eliminacion = st.checkbox(
                        "Confirmo que deseo eliminar este local",
                        key=f"confirm_delete_{id_local}"
                    )

                    if st.button(
                        "🗑️ Eliminar Local",
                        type="secondary",
                        use_container_width=True,
                        disabled=not confirmar_eliminacion
                    ):
                        ok, msg = svc_loc_eliminar_local(id_local)
                        (st.success if ok else st.error)(msg)
                        if ok:
                            st.rerun()
    else:
        st.info("No hay locales cargados.")

# -------------------------------
# DASHBOARD
# -------------------------------
elif menu == "🏠 Dashboard":
    st.header("📊 Dashboard General")
    
    stats = get_dashboard_stats()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📦 Total Enviadas",
            value=stats['total_enviadas'],
            delta=None
        )
    
    with col2:
        st.metric(
            label="✅ Total Devueltas", 
            value=stats['total_devueltas'],
            delta=None
        )
    
    with col3:
        st.metric(
            label="⚠️ Pendientes",
            value=stats['pendientes'],
            delta=None
        )
    
    with col4:
        st.metric(
            label="🚛 Viajes Activos",
            value=stats['viajes_activos'],
            delta=None
        )
    
    st.divider()
    
    # Solo gráfico de estado general y detalle (se elimina el gráfico por local)
    st.subheader("📈 Estado de Cajas")
    if stats['total_enviadas'] > 0:
        fig = go.Figure(data=[go.Pie(
            labels=['Devueltas', 'Pendientes'],
            values=[stats['total_devueltas'], stats['pendientes']],
            hole=.3,
            marker_colors=["#16a34a", "#f59e0b"]
        )])
        fig.update_layout(height=300, margin=dict(t=10,b=10,l=10,r=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay datos para mostrar")

    pend_local = get_pendientes_por_local()
    
    st.subheader("🔍 Detalle de Cajas Pendientes por Local")
    pend_local_det = pend_local[pend_local['pendientes'] > 0] if not pend_local.empty else pend_local
    if not pend_local_det.empty:
        st.dataframe(
            pend_local_det[['local', 'enviadas', 'devueltas', 'pendientes']].sort_values('pendientes', ascending=False),
            use_container_width=True,
            column_config={
                "local": "🏪 Local",
                "enviadas": st.column_config.NumberColumn("📦 Enviadas"),
                "devueltas": st.column_config.NumberColumn("✅ Devueltas"),
                "pendientes": st.column_config.NumberColumn("⚠️ Pendientes")
            }
        )
    else:
        st.success("🎉 No hay cajas pendientes por devolver en ningún local")

    # Utilidades ligeras de exportación/backup
    with st.expander("⬇️ Exportar / Backup", expanded=False):
        st.caption("Descarga rápida de datos para análisis histórico o respaldo.")
        col_exp1, col_exp2 = st.columns(2)
        with col_exp1:
            try:
                conn = get_connection()
                # Exportar choferes
                df_ch = pd.read_sql_query("SELECT * FROM choferes", conn)
                st.download_button(
                    "Choferes (CSV)",
                    data=df_ch.to_csv(index=False).encode("utf-8"),
                    file_name="choferes.csv",
                    mime="text/csv",
                    key="csv_choferes_dl",
                    use_container_width=True,
                )
                # Exportar locales
                df_loc = pd.read_sql_query("SELECT * FROM reception_local", conn)
                st.download_button(
                    "Locales (CSV)",
                    data=df_loc.to_csv(index=False).encode("utf-8"),
                    file_name="locales.csv",
                    mime="text/csv",
                    key="csv_locales_dl",
                    use_container_width=True,
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        with col_exp2:
            try:
                conn = get_connection()
                # Resumen de viajes (similar a get_viajes_detallados sin filtros)
                query_resumen = """
                    SELECT 
                        v.id,
                        v.fecha_viaje,
                        v.estado,
                        c.nombre as chofer_nombre,
                        COUNT(vl.id) as total_locales,
                        SUM(vl.cajas_enviadas) as total_enviadas,
                        SUM(vl.cajas_devueltas) as total_devueltas
                    FROM viajes v
                    LEFT JOIN choferes c ON v.chofer_id = c.id
                    LEFT JOIN viaje_locales vl ON v.id = vl.viaje_id
                    GROUP BY v.id
                    ORDER BY v.fecha_viaje DESC
                """
                df_vr = pd.read_sql_query(query_resumen, conn)
                st.download_button(
                    "Viajes (resumen CSV)",
                    data=df_vr.to_csv(index=False).encode("utf-8"),
                    file_name="viajes_resumen.csv",
                    mime="text/csv",
                    key="csv_viajes_resumen_dl",
                    use_container_width=True,
                )

                # Detalle de locales por viaje (incluye fecha y chofer)
                query_detalle = """
                    SELECT 
                        v.id as viaje_id,
                        v.fecha_viaje,
                        c.nombre as chofer_nombre,
                        vl.id as viaje_local_id,
                        vl.numero_local,
                        vl.cajas_enviadas,
                        vl.cajas_devueltas
                    FROM viaje_locales vl
                    JOIN viajes v ON vl.viaje_id = v.id
                    LEFT JOIN choferes c ON v.chofer_id = c.id
                    ORDER BY v.fecha_viaje DESC, vl.id
                """
                df_vl = pd.read_sql_query(query_detalle, conn)
                st.download_button(
                    "Viajes (detalle CSV)",
                    data=df_vl.to_csv(index=False).encode("utf-8"),
                    file_name="viajes_detalle.csv",
                    mime="text/csv",
                    key="csv_viajes_detalle_dl",
                    use_container_width=True,
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        # Backup del archivo .db completo
        try:
            db_path = get_db_path()
            with open(db_path, "rb") as f:
                db_bytes = f.read()
            st.download_button(
                "Base completa (.db)",
                data=db_bytes,
                file_name="cajas_plasticas_backup.db",
                mime="application/octet-stream",
                key="db_backup_dl",
                use_container_width=True,
            )
        except Exception as e:
            st.warning(f"No se pudo preparar el backup de la base: {e}")

    # Herramienta de limpieza (solo visible para admin)
    current_role = st.session_state.get("user", {}).get("role")
    if current_role == "admin":
        with st.expander("🧹 Limpiar datos (mantener Locales)", expanded=False):
            st.caption("Elimina viajes, sus ítems, choferes y despachos del CD. Mantiene la lista de Locales.")
            try:
                conn = get_connection()
                n_viajes = conn.execute("SELECT COUNT(*) FROM viajes").fetchone()[0]
                n_vl = conn.execute("SELECT COUNT(*) FROM viaje_locales").fetchone()[0]
                n_ch = conn.execute("SELECT COUNT(*) FROM choferes").fetchone()[0]
                n_cd = conn.execute("SELECT COUNT(*) FROM cd_despachos").fetchone()[0]
                n_cd_ori = conn.execute("SELECT COUNT(*) FROM cd_envios_origen").fetchone()[0]
                conn.close()
            except Exception:
                n_viajes = n_vl = n_ch = n_cd = n_cd_ori = 0
            st.write(f"Viajes: {n_viajes} · Ítems: {n_vl} · Choferes: {n_ch} · Despachos CD: {n_cd} · Envíos a Origen: {n_cd_ori}")
            del_ch = st.checkbox("Eliminar Choferes también", value=True)
            confirm = st.checkbox("Entiendo que esto es irreversible", key="purge_confirm_all")
            if st.button("🗑️ Borrar todo salvo Locales", type="secondary", disabled=not confirm):
                # Server-side enforcement también
                if st.session_state.get("user", {}).get("role") != "admin":
                    st.error("Acción no permitida")
                else:
                    try:
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute("BEGIN")
                        # Orden: tablas hijas primero para respetar foreign keys
                        cur.execute("DELETE FROM devoluciones_log")
                        cur.execute("DELETE FROM viaje_locales")
                        cur.execute("DELETE FROM viajes")
                        if del_ch:
                            cur.execute("DELETE FROM choferes")
                        cur.execute("DELETE FROM cd_despachos")
                        cur.execute("DELETE FROM cd_envios_origen")
                        cur.execute("COMMIT")
                        conn.close()
                        st.success("Datos eliminados. Los Locales se mantuvieron intactos.")
                        st.balloons(); st.rerun()
                    except Exception as e:
                        try:
                            cur.execute("ROLLBACK")
                        except Exception:
                            pass
                        try:
                            conn.close()
                        except Exception:
                            pass
                        st.error(f"No se pudieron eliminar los datos: {e}")

# -------------------------------
# CHOFERES
# -------------------------------
elif menu == "👷 Choferes":
    st.markdown("## 👷 Gestión de Choferes")
    st.markdown("Administra la información de los choferes del sistema")
    
    with st.container():
        st.markdown('<div class="form-container">', unsafe_allow_html=True)
        st.markdown("### ➕ Agregar Nuevo Chofer")
        
        with st.form("nuevo_chofer", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                nombre = st.text_input(
                    "👤 Nombre del Chofer*", 
                    placeholder="Ingresa el nombre completo"
                )
            with col2:
                contacto = st.text_input(
                    "📞 Contacto", 
                    placeholder="Teléfono o email"
                )
            
            col1, col2, col3 = st.columns([1, 1, 2])
            with col2:
                submit = st.form_submit_button(
                    "➕ Agregar Chofer", 
                    use_container_width=True,
                    type="primary"
                )

            if submit:
                if nombre.strip() == "":
                    st.error("⚠️ El nombre es obligatorio")
                else:
                    ok, msg = svc_ch_crear_chofer(nombre.strip(), contacto)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)

    # (Fin de sección Choferes)

elif menu == "🛣️ Viajes":
    st.markdown("## 🛣️ Gestión de Viajes")
    st.markdown("Administra los viajes y el reparto de cajas")

    tab1, tab2 = st.tabs(["➕ Nuevo Viaje", "📋 Historial de Viajes"])

    with tab1:
        choferes = svc_ch_listar_choferes()
        if choferes.empty:
            st.markdown("""
            <div class="warning-box">
                <h4>⚠️ No hay choferes disponibles</h4>
                <p>Primero debes registrar choferes en la sección correspondiente.</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            with st.container():
                st.markdown('<div class="form-container">', unsafe_allow_html=True)

                st.markdown("### 🚀 Crear Nuevo Viaje")

                col1, col2 = st.columns(2)
                with col1:
                    chofer_id = st.selectbox(
                        "🚛 Selecciona Chofer",
                        choferes["id"],
                        format_func=lambda x: f"👷 {choferes[choferes['id'] == x]['nombre'].iloc[0]}"
                    )
                with col2:
                    fecha = st.date_input("📅 Fecha del Viaje", datetime.date.today())

                st.markdown("### 🏪 Configuración de Locales")
                st.markdown("Agrega los locales y la cantidad de cajas para cada uno:")

                catalogo_locales = get_locales_catalogo()
                placeholder_local = "— Seleccionar —"
                if isinstance(catalogo_locales, pd.DataFrame):
                    # Versión que devuelve DataFrame: construir lista desde columnas
                    if not catalogo_locales.empty:
                        opciones_locales = [
                            placeholder_local
                        ] + [
                            f"{int(r['numero']) if str(r['numero']).isdigit() else r['numero']} - {r['nombre']}"
                            if r['nombre'] else str(r['numero'])
                            for _, r in catalogo_locales.iterrows()
                        ]
                    else:
                        opciones_locales = [placeholder_local]
                else:
                    # Versión lista de dicts {'numero','nombre','display'}
                    opciones_locales = [placeholder_local] + ([item.get("display","") for item in (catalogo_locales or [])])

                if "nuevo_viaje_items" not in st.session_state:
                    st.session_state["nuevo_viaje_items"] = []

                with st.form("form_item_viaje", clear_on_submit=True):
                    col_i1, col_i2, col_i3 = st.columns([3, 1.5, 1])
                    with col_i1:
                        local_sel = st.selectbox(
                            "Local",
                            opciones_locales,
                            index=0,
                            key="item_local_select"
                        )
                    with col_i2:
                        cajas_item = st.number_input(
                            "Cajas",
                            min_value=0,
                            step=1,
                            value=0,
                            key="item_cajas_input"
                        )
                    with col_i3:
                        add_clicked = st.form_submit_button("➕ Agregar", use_container_width=True)

                if add_clicked:
                    if local_sel == placeholder_local or cajas_item <= 0:
                        st.warning("Selecciona un local y una cantidad mayor a 0")
                    else:
                        ya_esta = any(str(it.get("numero_local")) == str(local_sel) for it in st.session_state.get("nuevo_viaje_items", []))
                        if ya_esta:
                            st.warning("Ese local ya fue agregado a la lista")
                        else:
                            st.session_state["nuevo_viaje_items"].append({
                                "numero_local": local_sel,
                                "cajas_enviadas": int(cajas_item)
                            })

                # Normalizar ítems antiguos (compatibilidad) a la nueva forma {numero_local, cajas_enviadas}
                norm_items = []
                for it in st.session_state["nuevo_viaje_items"]:
                    if "numero_local" in it and "cajas_enviadas" in it:
                        norm_items.append({"numero_local": it["numero_local"], "cajas_enviadas": int(it["cajas_enviadas"] or 0)})
                    else:
                        # Forma antigua con emojis
                        nl = it.get("🏪 Local")
                        cj = int(it.get("📦 Cajas") or 0)
                        norm_items.append({"numero_local": nl, "cajas_enviadas": cj})
                st.session_state["nuevo_viaje_items"] = norm_items
                items = norm_items
                if not items:
                    st.info("No hay ítems agregados todavía.")
                else:
                    hc1, hc2, hc3 = st.columns([6, 2, 1])
                    with hc1:
                        st.markdown("**🏪 Local**")
                    with hc2:
                        st.markdown("**📦 Cajas**")
                    with hc3:
                        st.markdown("**Acciones**")

                    for idx, it in enumerate(items):
                        rc1, rc2, rc3 = st.columns([6, 2, 1])
                        with rc1:
                            st.text(str(it.get("numero_local", "")))
                        with rc2:
                            st.text(str(it.get("cajas_enviadas", "")))
                        with rc3:
                            if st.button("🗑️ Eliminar", key=f"del_item_{idx}", use_container_width=True):
                                try:
                                    st.session_state["nuevo_viaje_items"].pop(idx)
                                except Exception:
                                    pass
                                st.rerun()

                col1, col2, col3 = st.columns([1, 1, 1])
                with col2:
                    crear_items_clicked = st.button(
                        "🚀 Crear Viaje",
                        use_container_width=True,
                        type="primary",
                        key="btn_crear_viaje_items"
                    )

                if crear_items_clicked:
                    locales = [
                        {"numero_local": it["numero_local"], "cajas_enviadas": int(it["cajas_enviadas"])}
                        for it in st.session_state["nuevo_viaje_items"]
                        if it.get("numero_local") and int(it.get("cajas_enviadas") or 0) > 0
                    ]
                    if locales:
                        viaje_id = svc_crear_viaje(chofer_id, fecha, locales)
                        st.success(f"✅ Viaje #{viaje_id} creado exitosamente")
                        st.balloons()
                        st.session_state["nuevo_viaje_items"] = []
                        st.rerun()
                    else:
                        st.error("⚠️ Debes agregar al menos un local con cajas")

                st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        st.markdown("### 📋 Historial de Viajes")
        with st.container():
            st.markdown('<div class="form-container">', unsafe_allow_html=True)
            st.markdown("#### 🔍 Filtros de Búsqueda")
            col1, col2, col3 = st.columns(3)
            with col1:
                fecha_inicio = st.date_input("📅 Fecha Inicio", value=datetime.date.today() - timedelta(days=30), key="viajes_fecha_inicio")
            with col2:
                fecha_fin = st.date_input("📅 Fecha Fin", value=datetime.date.today(), key="viajes_fecha_fin")
            with col3:
                choferes = svc_ch_listar_choferes()
                chofer_options = pd.concat([pd.DataFrame({"id": [None], "nombre": ["Todos"]}), choferes[["id", "nombre"]]])
                chofer_id = st.selectbox(
                    "👷 Chofer",
                    chofer_options["id"],
                    format_func=lambda x: "Todos" if x is None else chofer_options[chofer_options["id"] == x]["nombre"].iloc[0],
                    key="viajes_chofer"
                )
            estado = st.selectbox("📊 Estado", ["Todos", "En Curso", "Completado"], key="viajes_estado")
            st.markdown('</div>', unsafe_allow_html=True)

        viajes = svc_listar_viajes(
            fecha_desde=fecha_inicio,
            fecha_hasta=fecha_fin,
            chofer_id=chofer_id,
            estado=estado
        )

        if not viajes.empty:
            for _, row in viajes.iterrows():
                with st.container():
                    st.markdown('<div class="viaje-card">', unsafe_allow_html=True)
                    col1, col2, col3 = st.columns([2, 2, 1])
                    with col1:
                        st.markdown(f"### 🛣️ Viaje #{row['id']}")
                        st.markdown(f"**👷 Chofer:** {row['chofer']}")
                        st.markdown(f"**📅 Fecha:** {row['fecha_viaje']}")
                    with col2:
                        estado_color = "status-activo" if row['estado'] == "En Curso" else "status-completado"
                        st.markdown(f'<div style="text-align: center;"><span class="status-badge {estado_color}">{row["estado"]}</span></div>', unsafe_allow_html=True)
                        st.markdown(f"**🏪 Locales:** {row['total_locales'] or 0}")
                    with col3:
                        pendientes = (row['pendientes'] or 0)
                        if pendientes > 0:
                            st.metric("⚠️ Pendientes", pendientes)
                        else:
                            st.success("🎉 Completo")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📦 Enviadas", row['total_enviadas'] or 0)
                    with col2:
                        st.metric("✅ Devueltas", row['total_devueltas'] or 0)
                    with col3:
                        st.metric("🚛 Locales", row['total_locales'] or 0)
                    with st.expander(f"Ver detalles completos del viaje #{row['id']}", expanded=False):
                        locales = svc_viaje_locales(row['id'])
                        if not locales.empty:
                            locales['pendientes'] = locales['cajas_enviadas'] - locales['cajas_devueltas']
                            st.markdown("#### 📊 Detalle por Local")
                            st.dataframe(
                                locales[['numero_local', 'cajas_enviadas', 'cajas_devueltas', 'pendientes']],
                                use_container_width=True,
                                column_config={
                                    "numero_local": st.column_config.TextColumn("🏪 Local"),
                                    "cajas_enviadas": st.column_config.NumberColumn("📦 Enviadas"),
                                    "cajas_devueltas": st.column_config.NumberColumn("✅ Devueltas"),
                                    "pendientes": st.column_config.NumberColumn("⚠️ Pendientes")
                                }
                            )
                        st.markdown("#### ⚙️ Acciones del Viaje")
                        col_btn1, col_btn2, col_btn3 = st.columns(3)
                        with col_btn1:
                            if row['estado'] == 'En Curso':
                                if st.button("✅ Marcar Completado", key=f"complete_{row['id']}", type="primary"):
                                    svc_actualizar_estado_viaje(row['id'], 'Completado')
                                    st.success("✅ Estado actualizado a Completado")
                                    st.rerun()
                        with col_btn2:
                            if row['estado'] == 'Completado':
                                if st.button("🔄 Reactivar Viaje", key=f"reactivate_{row['id']}", type="secondary"):
                                    svc_actualizar_estado_viaje(row['id'], 'En Curso')
                                    # Activar modo edición de devueltas para este viaje tras reactivación
                                    st.session_state["edit_devueltas_viaje_id"] = row['id']
                                    st.success("🔄 Viaje reactivado.")
                                    st.rerun()
                        with col_btn3:
                            if st.button("🗑️ Eliminar Viaje", key=f"del_viaje_{row['id']}", type="secondary"):
                                svc_eliminar_viaje(row['id'])
                                st.success("🗑️ Viaje eliminado")
                                st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)
                    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="info-box">
                <h4>📝 No hay viajes que coincidan con los filtros</h4>
                <p>Ajusta los filtros o crea un nuevo viaje.</p>
            </div>
            """, unsafe_allow_html=True)

# -------------------------------
# CENTRO DE DISTRIBUCIÓN
# -------------------------------
elif menu == "🏬 Centro de Distribución":
    st.markdown("## 🏬 Centro de Distribución")
    st.markdown("Carga aquí los despachos del CD a los locales. El CD se detecta automáticamente.")
    role = st.session_state.get("user", {}).get("role", "")
    cd_edit_enabled = role in ("admin", "cd_only")

    # KPIs del CD: recibido (por viajes al CD), enviados, devueltos, stock actual
    tot = svc_cd_totales()  # se recalcula en cada run
    cd_name = tot.get("cd") or "CD"
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        st.metric("📥 Recibido en CD (viajes)", tot.get("recibido_viajes", 0))
    with k2:
        st.metric("📦 Enviados desde CD", tot.get("enviados", 0))
    with k3:
        st.metric("✅ Devueltos al CD", tot.get("devueltos", 0))
    with k4:
        st.metric("🏷️ CD detectado", cd_name)
    with k5:
        st.metric("📊 Stock CD", tot.get("stock", 0))
    with k6:
        st.metric("📤 Enviado a Pastas Frescas", tot.get("enviados_origen", 0))

    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)

    # Formulario para registrar un despacho desde CD a un destino (sin origen seleccionable)
    st.markdown("### ➕ Registrar despacho del CD")
    catalogo_df_cd = get_locales_catalogo()
    catalogo_cd_list = []
    if not catalogo_df_cd.empty:
        for _, r in catalogo_df_cd.iterrows():
            display = f"{int(r['numero']) if str(r['numero']).isdigit() else r['numero']} - {r['nombre']}" if r['nombre'] else str(r['numero'])
            catalogo_cd_list.append({
                "numero": r['numero'],
                "nombre": r['nombre'],
                "display": display
            })
    opciones = ["— Seleccionar —"] + [it["display"] for it in catalogo_cd_list]

    # UI con pestañas para que se vea como 'Gestión de Viajes'
    tab_nuevo, tab_hist = st.tabs(["➕ Nuevo despacho", "🗂️ Historial de despachos"]) 

    with tab_nuevo:
        with st.form("form_cd_despacho", clear_on_submit=True):
            destino = st.selectbox("Destino (local)", opciones)
            c1, c2 = st.columns(2)
            with c1:
                fecha_cd = st.date_input("📅 Fecha", value=datetime.date.today())
            with c2:
                cajas_cd = st.number_input("📦 Cajas", min_value=0, step=1, value=0)
            submitted_cd = st.form_submit_button("Registrar despacho", type="primary", disabled=(not cd_edit_enabled))

        if submitted_cd and cd_edit_enabled:
            if destino != "— Seleccionar —" and cajas_cd > 0:
                ok, msg = svc_cd_crear_despacho(cd_name, destino, fecha_cd, int(cajas_cd))
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
            else:
                st.warning("Selecciona destino y una cantidad mayor a 0.")
        elif submitted_cd and not cd_edit_enabled:
            st.warning("Tu rol no permite crear o editar en el Centro de Distribución.")

        st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
        st.markdown("### 📋 Despachos pendientes")
        f1, f2, f3 = st.columns(3)
        with f1:
            fecha_i = st.date_input("Desde", value=datetime.date.today() - timedelta(days=30), key="cd_pend_from")
        with f2:
            fecha_f = st.date_input("Hasta", value=datetime.date.today(), key="cd_pend_to")
        with f3:
            filtro_dest = st.text_input("Buscar destino", value="", key="cd_pend_search")

        # Cálculo y render de pendientes dentro de la misma pestaña
        df_list = svc_cd_listar_despachos(start_date=fecha_i, end_date=fecha_f)
        if filtro_dest:
            df_list = df_list[df_list["destino_local"].str.contains(filtro_dest, case=False, na=False)]
        df_list = df_list.copy()
        if not df_list.empty:
            df_list["cajas_enviadas"] = df_list["cajas_enviadas"].fillna(0).astype(int)
            df_list["cajas_devueltas"] = df_list["cajas_devueltas"].fillna(0).astype(int)
            df_list["pendientes"] = df_list["cajas_enviadas"] - df_list["cajas_devueltas"]
        else:
            df_list["pendientes"] = pd.Series(dtype=int)

        df_pend = df_list[df_list["pendientes"] > 0]
        if df_pend.empty:
            st.info("No hay despachos pendientes en este rango.")
        else:
            cols = st.columns(2)
            for idx, (_, r) in enumerate(df_pend.iterrows()):
                pendientes = int(r["pendientes"])
                with cols[idx % 2]:
                    st.markdown('<div class="professional-card">', unsafe_allow_html=True)
                    top = f"<strong>#{int(r['id'])}</strong> · {r['fecha']} · 🏪 {r['destino_local']}"
                    st.markdown(top, unsafe_allow_html=True)
                    colx, coly, colz = st.columns(3)
                    with colx:
                        st.metric("📦 Enviadas", int(r['cajas_enviadas'] or 0))
                    with coly:
                        st.metric("✅ Devueltas", int(r['cajas_devueltas'] or 0))
                    with colz:
                        st.metric("⚠️ Pendientes", pendientes)
                    with st.form(f"cd_dev_{int(r['id'])}"):
                        cant = st.number_input(
                            "Cantidad devuelta",
                            min_value=0,
                            max_value=pendientes,
                            step=1,
                            value=0,
                            key=f"cd_dev_{int(r['id'])}_qty",
                        )
                        left, right = st.columns(2)
                        with left:
                            if st.form_submit_button("📥 Registrar devolución", use_container_width=True, disabled=(not cd_edit_enabled)):
                                if cant > 0:
                                    svc_cd_registrar_devolucion(int(r['id']), int(cant))
                                    st.success("Devolución registrada.")
                                    st.rerun()
                                else:
                                    st.warning("Ingresa una cantidad mayor a 0")
                        with right:
                            if st.form_submit_button("🗑️ Eliminar despacho", use_container_width=True, disabled=(not cd_edit_enabled)):
                                ok2, msg = svc_cd_eliminar_despacho_forzado(int(r['id']))
                                if ok2:
                                    st.success("Despacho eliminado.")
                                    st.rerun()
                                else:
                                    st.error(msg)
                    if not cd_edit_enabled:
                        st.caption("Visualización de solo lectura por rol: no puedes registrar devoluciones ni eliminar.")
                    st.markdown('</div>', unsafe_allow_html=True)

    with tab_hist:
        f1, f2, f3 = st.columns(3)
        with f1:
            fecha_i_h = st.date_input("Desde", value=datetime.date.today() - timedelta(days=30), key="cd_hist_from")
        with f2:
            fecha_f_h = st.date_input("Hasta", value=datetime.date.today(), key="cd_hist_to")
        with f3:
            filtro_dest_hist = st.text_input("🔎 Buscar por destino", value="", key="cd_hist_search")

        df_list_h = svc_cd_listar_despachos(start_date=fecha_i_h, end_date=fecha_f_h)
        if filtro_dest_hist:
            df_list_h = df_list_h[df_list_h["destino_local"].str.contains(filtro_dest_hist, case=False, na=False)]
        df_list_h = df_list_h.copy()
        if not df_list_h.empty:
            df_list_h["cajas_enviadas"] = df_list_h["cajas_enviadas"].fillna(0).astype(int)
            df_list_h["cajas_devueltas"] = df_list_h["cajas_devueltas"].fillna(0).astype(int)
            df_list_h["pendientes"] = df_list_h["cajas_enviadas"] - df_list_h["cajas_devueltas"]
        else:
            df_list_h["pendientes"] = pd.Series(dtype=int)

        # Historial = completados (pendientes == 0)
        df_hist = df_list_h[df_list_h["pendientes"] == 0].copy()
        if df_hist.empty:
            st.info("Aún no hay despachos completados en este rango.")
        else:
            if not cd_edit_enabled:
                st.info("Para editar necesitas rol 'admin' o 'cd_only'.")

            df_hist_view = df_hist[["id", "fecha", "destino_local", "cajas_enviadas", "cajas_devueltas"]].rename(columns={
                "id": "#", "fecha": "📅 Fecha", "destino_local": "🏪 Destino", "cajas_enviadas": "📦 Enviadas", "cajas_devueltas": "✅ Devueltas"
            }).copy()

            try:
                df_hist_view["📅 Fecha"] = pd.to_datetime(df_hist_view["📅 Fecha"]).dt.date
            except Exception:
                pass

            df_hist_view["🗑️ Eliminar"] = False

            if cd_edit_enabled:
                column_config = {
                    "#": st.column_config.NumberColumn("#", disabled=True),
                    "🏪 Destino": st.column_config.TextColumn("🏪 Destino", disabled=True),
                    "📦 Enviadas": st.column_config.NumberColumn("📦 Enviadas", min_value=1, step=1),
                    "✅ Devueltas": st.column_config.NumberColumn("✅ Devueltas", min_value=0, step=1),
                    "🗑️ Eliminar": st.column_config.CheckboxColumn("🗑️ Eliminar")
                }
                if pd.api.types.is_datetime64_any_dtype(df_hist_view["📅 Fecha"]) or isinstance(df_hist_view["📅 Fecha"].iloc[0] if not df_hist_view.empty else None, (pd.Timestamp, datetime.date)):
                    column_config["📅 Fecha"] = st.column_config.DateColumn("📅 Fecha", format="YYYY-MM-DD")
                else:
                    column_config["📅 Fecha"] = st.column_config.TextColumn("📅 Fecha")
                edited_hist = st.data_editor(
                    df_hist_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_config,
                    key="cd_hist_editor"
                )
            else:
                st.dataframe(
                    df_hist_view.drop(columns=["🗑️ Eliminar"]),
                    use_container_width=True,
                    hide_index=True
                )
                edited_hist = df_hist_view.copy()

            applied = st.button("Aplicar cambios", key="cd_hist_apply_changes", disabled=(not cd_edit_enabled))
            if applied and cd_edit_enabled:
                to_delete = edited_hist[edited_hist["🗑️ Eliminar"] == True]
                ok_del, err_del = 0, []
                for _, row in to_delete.iterrows():
                    rid = int(row["#"])
                    ok, msg = svc_cd_revertir_despacho_a_pendiente(rid)
                    if ok:
                        ok_del += 1
                    else:
                        err_del.append(f"Despacho #{rid}: {msg}")
                base = df_hist_view.set_index("#")
                edited_idx = edited_hist.set_index("#")
                ok_upd, err_upd = 0, []
                for rid, row in edited_idx.iterrows():
                    if bool(row.get("🗑️ Eliminar", False)):
                        continue
                    fecha_new = row["📅 Fecha"]
                    try:
                        fecha_new = pd.to_datetime(fecha_new).date()
                    except Exception:
                        pass
                    enviadas_new = int(row["📦 Enviadas"])
                    devueltas_new = int(row["✅ Devueltas"])
                    orig = base.loc[rid]
                    changed = (
                        str(fecha_new) != str(orig["📅 Fecha"]) or
                        enviadas_new != int(orig["📦 Enviadas"]) or
                        devueltas_new != int(orig["✅ Devueltas"]) 
                    )
                    if not changed:
                        continue
                    if devueltas_new > enviadas_new:
                        err_upd.append(f"Despacho #{rid}: Devueltas ({devueltas_new}) no pueden superar Enviadas ({enviadas_new})")
                        continue
                    ok, msg = svc_cd_actualizar_despacho_detallado(int(rid), fecha_new, enviadas_new, devueltas_new)
                    if ok:
                        ok_upd += 1
                    else:
                        err_upd.append(f"Despacho #{rid}: {msg}")
                if ok_del:
                    st.success(f"✅ {ok_del} despacho(s) revertido(s) a estado pendiente")
                if ok_upd:
                    st.success(f"✅ Actualizados {ok_upd} despacho(s)")
                if err_del or err_upd:
                    for m in err_del + err_upd:
                        st.error(m)
                if ok_del or ok_upd:
                    st.rerun()
            elif applied and not cd_edit_enabled:
                st.warning("No tienes permisos para aplicar cambios.")

    # Sección Envíos a Pastas Frescas en pestañas
    st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
    tab_pf_new, tab_pf_hist = st.tabs(["📤 Enviar a Pastas Frescas", "🗂️ Historial PF"]) 

    with tab_pf_new:
        with st.form("form_cd_origen", clear_on_submit=True):
            col_a, col_b = st.columns(2)
            with col_a:
                fecha_origen = st.date_input("📅 Fecha", value=datetime.date.today(), key="cd_origen_fecha")
            with col_b:
                cajas_origen = st.number_input("📦 Cajas a enviar", min_value=0, step=1, value=0, key="cd_origen_cajas")
            submit_origen = st.form_submit_button("Enviar a Pastas Frescas", type="primary", disabled=(not cd_edit_enabled))
        if submit_origen and cd_edit_enabled:
            if cajas_origen > 0:
                ok, msg = svc_cd_enviar_a_origen(fecha_origen, int(cajas_origen))
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
            else:
                st.warning("Ingresa una cantidad mayor a 0.")
        elif submit_origen and not cd_edit_enabled:
            st.warning("Tu rol no permite crear o editar en el Centro de Distribución.")

    with tab_pf_hist:
        h1, h2 = st.columns(2)
        with h1:
            pf_i = st.date_input("Desde", value=datetime.date.today() - timedelta(days=30), key="pf_hist_from")
        with h2:
            pf_f = st.date_input("Hasta", value=datetime.date.today(), key="pf_hist_to")
        
        # Mostrar stock actual para orientar al usuario
        if cd_edit_enabled:
            stock_actual = svc_cd_totales().get("stock", 0)
            st.info(f"📦 Stock actual en CD: **{stock_actual}** cajas disponibles")
        
        df_pf = svc_cd_listar_envios_origen(start_date=pf_i, end_date=pf_f)
        if df_pf.empty:
            st.info("No hay envíos a Pastas Frescas en el rango seleccionado.")
        else:
            # Agrupar por fecha y sumar las cajas enviadas
            df_pf_grouped = df_pf.groupby('fecha').agg({
                'cajas_enviadas': 'sum',
                'id': lambda x: list(x)  # Guardar lista de IDs para poder eliminar después
            }).reset_index()
            
            # Preparar editor inline con datos agrupados
            df_pf_view = df_pf_grouped[["fecha", "cajas_enviadas", "id"]].rename(columns={
                "fecha": "📅 Fecha", "cajas_enviadas": "📦 Enviadas", "id": "ids_envios"
            }).copy()
            
            # Convertir fecha a tipo datetime para que sea compatible con DateColumn
            try:
                df_pf_view["📅 Fecha"] = pd.to_datetime(df_pf_view["📅 Fecha"]).dt.date
            except Exception:
                # Si falla la conversión, mantener como string pero usar TextColumn
                pass
            
            df_pf_view["🗑️ Eliminar"] = False
            
            if cd_edit_enabled:
                # Configuración de columnas para datos agrupados
                column_config = {
                    "📦 Enviadas": st.column_config.NumberColumn("📦 Enviadas", min_value=1, step=1),
                    "🗑️ Eliminar": st.column_config.CheckboxColumn("🗑️ Eliminar"),
                    "ids_envios": st.column_config.Column("IDs", disabled=True, width="small")  # Oculta pero mantiene los IDs
                }
                
                # Solo agregar DateColumn si la fecha está en formato correcto
                if pd.api.types.is_datetime64_any_dtype(df_pf_view["📅 Fecha"]) or isinstance(df_pf_view["📅 Fecha"].iloc[0] if not df_pf_view.empty else None, (pd.Timestamp, datetime.date)):
                    column_config["📅 Fecha"] = st.column_config.DateColumn("📅 Fecha", format="YYYY-MM-DD")
                else:
                    column_config["📅 Fecha"] = st.column_config.TextColumn("📅 Fecha")
                
                # Ocultar la columna de IDs del usuario pero mantenerla en el dataframe
                df_pf_display = df_pf_view.drop(columns=['ids_envios'])
                df_pf_display['🗑️ Eliminar'] = False
                
                edited_pf_display = st.data_editor(
                    df_pf_display,
                    use_container_width=True,
                    hide_index=True,
                    column_config={k: v for k, v in column_config.items() if k != 'ids_envios'},
                    key="pf_hist_editor"
                )
                
                # Reconstituir el dataframe completo con los IDs para procesamiento
                edited_pf = edited_pf_display.copy()
                edited_pf['ids_envios'] = df_pf_view['ids_envios'].values
            else:
                # Modo solo lectura - mostrar datos agrupados sin columnas de edición
                st.dataframe(
                    df_pf_view.drop(columns=["ids_envios"]),
                    use_container_width=True,
                    hide_index=True
                )
                # Para evitar errores en el código siguiente
                edited_pf = df_pf_view.copy()
                edited_pf['🗑️ Eliminar'] = False

            if st.button("Aplicar cambios", key="pf_hist_apply_changes", disabled=(not cd_edit_enabled)):
                # Aplicar eliminaciones primero - eliminar TODOS los envíos de las fechas marcadas
                to_delete = edited_pf[edited_pf["🗑️ Eliminar"] == True]
                ok_del, err_del = 0, []
                cajas_liberadas_por_eliminacion = 0
                
                for idx, row in to_delete.iterrows():
                    ids_to_delete = row["ids_envios"]  # Lista de IDs a eliminar para esta fecha
                    fecha = row["📅 Fecha"]
                    cajas_total = int(row["📦 Enviadas"])
                    
                    # Eliminar todos los envíos de esta fecha
                    for rid in ids_to_delete:
                        ok, msg = svc_cd_eliminar_envio_origen(int(rid))
                        if ok:
                            ok_del += 1
                        else:
                            err_del.append(f"Envío #{rid} (fecha {fecha}): {msg}")
                    
                    if ok_del > 0:
                        cajas_liberadas_por_eliminacion += cajas_total

                # Aplicar ediciones de cantidad (donde no se marcó eliminar)
                ok_upd, err_upd = 0, []
                
                for idx, row in edited_pf.iterrows():
                    if bool(row.get("🗑️ Eliminar", False)):
                        continue
                    
                    # Obtener datos originales y editados
                    fecha_new = row["📅 Fecha"]
                    cajas_new = int(row["📦 Enviadas"])
                    ids_envios = row["ids_envios"]
                    
                    # Encontrar la fila original correspondiente
                    orig_row = df_pf_view.iloc[idx]
                    cajas_old = int(orig_row["📦 Enviadas"])
                    
                    # Verificar si cambió la cantidad
                    if cajas_new != cajas_old:
                        # Si hay múltiples envíos en la misma fecha, necesitamos decidir cómo distribuir el cambio
                        # Por simplicidad, actualizamos el primer envío con la nueva cantidad total
                        # y eliminamos los demás envíos de esa fecha
                        
                        if len(ids_envios) == 1:
                            # Caso simple: solo un envío en esa fecha
                            ok, msg = svc_cd_actualizar_envio_origen(int(ids_envios[0]), fecha_new, cajas_new)
                            if ok:
                                ok_upd += 1
                            else:
                                err_upd.append(f"Fecha {fecha_new}: {msg}")
                        else:
                            # Caso complejo: múltiples envíos en la misma fecha
                            # Actualizar el primer envío con la cantidad total nueva
                            # y eliminar los demás
                            ok, msg = svc_cd_actualizar_envio_origen(int(ids_envios[0]), fecha_new, cajas_new)
                            if ok:
                                ok_upd += 1
                                # Eliminar los envíos adicionales
                                for rid in ids_envios[1:]:
                                    svc_cd_eliminar_envio_origen(int(rid))
                            else:
                                err_upd.append(f"Fecha {fecha_new}: {msg}")

                # Mostrar resultados
                if ok_del:
                    fechas_eliminadas = len(to_delete)
                    st.success(f"✅ Eliminados todos los envíos de {fechas_eliminadas} fecha(s) ({ok_del} envío(s) total)")
                if ok_upd:
                    st.success(f"✅ Actualizadas {ok_upd} fecha(s)")
                if err_del or err_upd:
                    for m in err_del + err_upd:
                        st.error(m)
                if ok_del or ok_upd:
                    st.rerun()

elif menu == "📥 Devoluciones":
    st.markdown("## 📥 Registro de Devoluciones")
    st.markdown("Registra las cajas devueltas por cada local")

    with st.container():
        st.markdown('<div class="form-container">', unsafe_allow_html=True)
        st.markdown("#### 🔍 Filtros de Búsqueda")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            fecha_inicio = st.date_input("📅 Fecha Inicio", 
                                       value=datetime.date.today() - timedelta(days=30),
                                       key="devoluciones_fecha_inicio")
        with col2:
            fecha_fin = st.date_input("📅 Fecha Fin", 
                                    value=datetime.date.today(),
                                    key="devoluciones_fecha_fin")
        with col3:
            choferes = svc_ch_listar_choferes()
            chofer_options = pd.concat([pd.DataFrame({"id": [None], "nombre": ["Todos"]}), choferes[["id", "nombre"]]])
            chofer_id = st.selectbox(
                "👷 Chofer",
                chofer_options["id"],
                format_func=lambda x: "Todos" if x is None else chofer_options[chofer_options["id"] == x]["nombre"].iloc[0],
                key="devoluciones_chofer"
            )
        
        st.markdown('</div>', unsafe_allow_html=True)

    viajes = svc_listar_viajes(
        fecha_desde=fecha_inicio,
        fecha_hasta=fecha_fin,
        chofer_id=chofer_id,
        estado="En Curso"
    )
    
    if viajes.empty:
        st.markdown("""
        <div class="warning-box">
            <h4>⚠️ No hay viajes activos disponibles</h4>
            <p>No hay viajes en curso que coincidan con los filtros. Ajusta los filtros o crea/reactiva un viaje.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        with st.container():
            st.markdown('<div class="form-container">', unsafe_allow_html=True)
            st.markdown("### 🛣️ Seleccionar Viaje")
            
            viaje_id = st.selectbox(
                "Elige el viaje para registrar devoluciones:",
                viajes["id"],
                format_func=lambda x: f"🚛 Viaje #{x} - {viajes[viajes['id']==x]['chofer'].iloc[0]} - {viajes[viajes['id']==x]['fecha_viaje'].iloc[0]}",
                label_visibility="collapsed"
            )
            st.markdown('</div>', unsafe_allow_html=True)
            locales = svc_viaje_locales(viaje_id)
            if locales.empty:
                st.markdown("""
                <div class="info-box">
                    <h4>🏪 Sin locales asignados</h4>
                    <p>Este viaje no tiene locales configurados.</p>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"### 🏪 Locales del Viaje #{viaje_id}")

                total_enviadas = locales['cajas_enviadas'].sum()
                total_devueltas = locales['cajas_devueltas'].sum()
                total_pendientes = total_enviadas - total_devueltas
                progreso = (total_devueltas / total_enviadas * 100) if total_enviadas > 0 else 0

                col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📦 Total Enviadas", total_enviadas)
            with col2:
                st.metric("✅ Total Devueltas", total_devueltas)
            with col3:
                st.metric("⚠️ Pendientes", total_pendientes)
            with col4:
                st.metric("📊 Progreso", f"{progreso:.1f}%")
            
            st.progress(progreso / 100)

            # Acción masiva: Entregar todas las pendientes del viaje
            if total_pendientes > 0:
                with st.container():
                    st.markdown(
                        "<div class=\"warning-box\"><strong>Acción rápida:</strong> Puedes registrar todas las cajas pendientes de este viaje de una sola vez.</div>",
                        unsafe_allow_html=True
                    )
                    colb1, colb2 = st.columns([1, 2])
                    with colb1:
                        confirmar_todas = st.checkbox("Confirmo entregar todas", key=f"confirm_all_{viaje_id}")
                    with colb2:
                        if st.button("📦 Entregar todas", type="primary", disabled=not confirmar_todas):
                            svc_registrar_devolucion_todas_por_viaje(viaje_id)
                            # Guardar bandera para mostrar prompt de finalización al recargar
                            st.session_state["finalize_prompt_viaje_id"] = viaje_id
                            st.success("✅ Se registraron todas las cajas pendientes como devueltas.")
                            st.rerun()
            
            # Si después de una acción masiva no quedan pendientes, preguntar si finalizar
            if total_pendientes == 0 and st.session_state.get("finalize_prompt_viaje_id") == viaje_id:
                with st.container():
                    st.markdown(
                        """
                        <div class=\"success-box\">
                            <h4>🎉 ¡Todas las cajas de este viaje fueron registradas como devueltas!</h4>
                            <p>¿Deseas marcar el viaje como <strong>Completado</strong> ahora?</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                    c1, c2 = st.columns([1, 1])
                    with c1:
                        if st.button("✅ Sí, finalizar viaje", key=f"finalizar_{viaje_id}", type="primary", use_container_width=True):
                            svc_actualizar_estado_viaje(viaje_id, 'Completado')
                            st.success("✅ Viaje finalizado exitosamente")
                            st.rerun()
                    with c2:
                        if st.button("⏳ Ahora no", key=f"no_finalizar_{viaje_id}", use_container_width=True):
                            try:
                                del st.session_state["finalize_prompt_viaje_id"]
                            except KeyError:
                                pass

            
            st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)
            
            # Determinar si estamos en modo edición por reactivación
            editing_mode = st.session_state.get("edit_devueltas_viaje_id") == viaje_id
            cols = st.columns(2)
            cambios_placeholder = st.empty()
            edited_items = []
            for idx, (_, row) in enumerate(locales.iterrows()):
                pendientes = int(row['cajas_enviadas'] - row['cajas_devueltas'])
                with cols[idx % 2]:
                    card_color = "#f59e0b" if pendientes > 0 else "#22c55e"
                    estado_label = "⚠️ Pendiente" if pendientes > 0 else "🎉 Completo"
                    st.markdown(f"""
                    <div class="professional-card" style="border-left: 4px solid {card_color};">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                            <h4 style="margin: 0; color: #1f2937;">🏪 {row['numero_local']}</h4>
                            <span class="status-badge {'status-pendiente' if pendientes>0 else 'status-activo'}">{estado_label}</span>
                        </div>
                        <div style="color: #6b7280; margin-bottom: 0.75rem;">
                            <div style="display: flex; justify-content: space-between;">
                                <span>📦 Enviadas:</span><strong>{int(row['cajas_enviadas'])}</strong>
                            </div>
                    """, unsafe_allow_html=True)
                    if editing_mode:
                        # input editable para devueltas
                        new_val = st.number_input(
                            f"Devueltas ({row['numero_local']})",
                            min_value=0,
                            max_value=int(row['cajas_enviadas']),
                            value=int(row['cajas_devueltas']),
                            step=1,
                            key=f"edit_dev_{viaje_id}_{row['id']}"
                        )
                        edited_items.append({
                            'id': int(row['id']),
                            'cajas_devueltas': int(new_val),
                            'original': int(row['cajas_devueltas'])
                        })
                    else:
                        st.markdown(f"""
                            <div style=\"display: flex; justify-content: space-between;\"><span>✅ Devueltas:</span><strong>{int(row['cajas_devueltas'])}</strong></div>
                        """, unsafe_allow_html=True)
                        # Formularios originales de registro sólo si hay pendientes
                        if pendientes > 0:
                            with st.form(f"devolucion_{row['id']}"):
                                cantidad = st.number_input(
                                    "Cantidad a devolver:",
                                    min_value=0,
                                    max_value=pendientes,
                                    step=1,
                                    value=0,
                                    key=f"input_dev_{row['id']}"
                                )
                                cform1, cform2 = st.columns(2)
                                with cform1:
                                    if st.form_submit_button("📥 Registrar", use_container_width=True, type="primary"):
                                        if cantidad > 0:
                                            svc_registrar_devolucion(row['id'], cantidad)
                                            st.success(f"✅ {cantidad} cajas registradas para {row['numero_local']}")
                                            st.rerun()
                                        else:
                                            st.warning("⚠️ Ingresa una cantidad mayor a 0")
                                with cform2:
                                    if st.form_submit_button("📥 Devolver Todas", use_container_width=True):
                                        svc_registrar_devolucion(row['id'], pendientes)
                                        st.success(f"✅ Todas las cajas ({pendientes}) registradas para {row['numero_local']}")
                                        st.rerun()
                    st.markdown(f"""
                        <div style="display: flex; justify-content: space-between; { 'color:#f59e0b;' if pendientes>0 else 'color:#22c55e;' }">
                            <span>{'⚠️ Pendientes:' if pendientes>0 else '🎉 Estado:'}</span><strong>{pendientes if pendientes>0 else 'Completo'}</strong>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.markdown('<div style="margin-bottom: 1rem;"></div>', unsafe_allow_html=True)

            if editing_mode:
                # Botón global para guardar cambios
                btn_disabled = False
                if st.button("💾 Guardar Cambios", type="primary", disabled=btn_disabled):
                    # Filtrar solo cambios reales
                    to_update = [
                        {'id': it['id'], 'cajas_devueltas': it['cajas_devueltas']}
                        for it in edited_items if it['cajas_devueltas'] != it['original']
                    ]
                    if not to_update:
                        st.info("No hay cambios para aplicar.")
                    else:
                        ok_upd, msg_upd = svc_update_devueltas_viaje_locales(viaje_id, to_update)
                        if ok_upd:
                            st.success(msg_upd)
                            # Determinar si con los nuevos valores el viaje quedó sin pendientes
                            # Recalcular pendientes usando los valores recien editados (aplicando cambios locales)
                            # Construimos un diccionario id->nuevo devueltas para calcular sin otra consulta
                            updated_map = {item['id']: item['cajas_devueltas'] for item in to_update}
                            nuevas_pendientes = 0
                            for _, rloc in locales.iterrows():
                                nuevas_dev = updated_map.get(int(rloc['id']), int(rloc['cajas_devueltas']))
                                pendientes_loc = int(rloc['cajas_enviadas']) - int(nuevas_dev)
                                if pendientes_loc > 0:
                                    nuevas_pendientes += pendientes_loc
                            if nuevas_pendientes == 0:
                                # Salimos de modo edición para que aparezca el banner de completado
                                try:
                                    if st.session_state.get("edit_devueltas_viaje_id") == viaje_id:
                                        del st.session_state["edit_devueltas_viaje_id"]
                                except KeyError:
                                    pass
                            # Re-render para mostrar estado actualizado / banner
                            st.rerun()
                        else:
                            st.error(msg_upd)
            
            
            # Ocultar banner de completado si estamos en modo edición aunque no haya pendientes
            if (total_pendientes == 0 and not editing_mode) and st.session_state.get("finalize_prompt_viaje_id") != viaje_id:
                st.markdown("""
                <div class="success-box">
                    <h4>🎉 ¡Viaje Completado!</h4>
                    <p>Todas las cajas han sido devueltas. ¿Te gustaría marcar este viaje como completado?</p>
                </div>
                """, unsafe_allow_html=True)
                
                if st.button("✅ Marcar Viaje como Completado", type="primary"):
                    svc_actualizar_estado_viaje(viaje_id, 'Completado')
                    st.success("✅ Viaje completado exitosamente")
                    st.rerun()

# -------------------------------
# USUARIOS (ADMIN)
# -------------------------------
elif menu == "👥 Usuarios":
    user = st.session_state.get("user") or {}
    if user.get("role") != "admin":
        st.error("No tienes permisos para acceder a esta sección.")
        st.stop()
    st.markdown("## 👥 Gestión de Usuarios")
    st.caption("Crear, editar, cambiar contraseñas y eliminar usuarios. Protegido el último administrador.")

    tab_new, tab_manage = st.tabs(["➕ Crear usuario", "🗂️ Gestionar usuarios"])

    with tab_new:
        with st.form("create_user_form", clear_on_submit=True):
            c1, c2 = st.columns([2, 1])
            with c1:
                u = st.text_input("Usuario")
            with c2:
                r = st.selectbox("Rol", VALID_ROLES, index=0)
            p1 = st.text_input("Contraseña", type="password")
            p2 = st.text_input("Repetir contraseña", type="password")
            submit_new = st.form_submit_button("Crear", type="primary")
        if submit_new:
            if not u or not p1 or not p2:
                st.warning("Completa todos los campos")
            elif p1 != p2:
                st.error("Las contraseñas no coinciden")
            else:
                ok, msg = create_user(u, p1, r)
                (st.success if ok else st.error)(msg)
                if ok:
                    st.rerun()

    with tab_manage:
        df_users = list_users()
        if df_users.empty:
            st.info("No hay usuarios registrados.")
        else:
            # Panel de depuración (solo admin): ver hash parcial para confirmar cambios de contraseña
            with st.expander("🛠️ Depuración (solo admin – ocultar luego)"):
                try:
                    conn = get_connection(); cur = conn.cursor()
                    rows = cur.execute("SELECT id, username, role, substr(password_hash,1,10) || '…' AS hash_preview FROM users ORDER BY username").fetchall()
                    conn.close()
                    if rows:
                        st.dataframe(
                            pd.DataFrame(rows, columns=["id","username","role","hash_preview"]),
                            hide_index=True,
                            use_container_width=True
                        )
                        st.caption("hash_preview cambia cuando actualizas la contraseña. Si no cambia, la actualización no se guardó.")
                except Exception as e:
                    st.warning(f"No se pudo cargar depuración: {e}")
            for _, row in df_users.iterrows():
                uid = int(row["id"]) 
                uname = str(row["username"]) 
                urole = str(row["role"]) 
                with st.container():
                    st.markdown('<div class="professional-card">', unsafe_allow_html=True)
                    c1, c2, c3, c4 = st.columns([3, 2, 3, 2])
                    with c1:
                        new_uname = st.text_input("Usuario", value=uname, key=f"usr_name_{uid}")
                    with c2:
                        new_role = st.selectbox("Rol", VALID_ROLES, index=list(VALID_ROLES).index(urole) if urole in VALID_ROLES else 0, key=f"usr_role_{uid}")
                    with c3:
                        st.markdown("**Cambiar contraseña**")
                        np1 = st.text_input("Nueva", type="password", key=f"usr_np1_{uid}")
                        np2 = st.text_input("Repetir", type="password", key=f"usr_np2_{uid}")
                    with c4:
                        st.markdown("**Acciones**")
                        save = st.button("💾 Guardar", key=f"usr_save_{uid}", use_container_width=True)
                        reset = st.button("🔑 Reset pass", key=f"usr_reset_{uid}", use_container_width=True)
                        delete = st.button("🗑️ Eliminar", key=f"usr_del_{uid}", use_container_width=True)

                    # Guardar cambios de usuario/rol (e integrar cambio de contraseña si se ingresó)
                    if save:
                        any_change = False
                        ok_u = True; msg_u = ""
                        if (new_uname != uname) or (new_role != urole):
                            ok_u, msg_u = update_user(uid, new_username=new_uname, new_role=new_role)
                            any_change = True
                        ok_p = True; msg_p = ""
                        if np1 or np2:
                            if not np1 or not np2:
                                ok_p, msg_p = False, "Ingresa y repite la nueva contraseña"
                            elif np1 != np2:
                                ok_p, msg_p = False, "Las contraseñas no coinciden"
                            else:
                                ok_p, msg_p = set_user_password(uid, np1)
                            any_change = True
                        if not any_change:
                            st.info("Sin cambios para guardar")
                        else:
                            # Mostrar mensajes combinados
                            msgs = []
                            if msg_u:
                                (st.success if ok_u else st.error)(msg_u)
                                msgs.append(msg_u)
                            if msg_p:
                                (st.success if ok_p else st.error)(msg_p)
                                msgs.append(msg_p)
                            if ok_u and ok_p:
                                st.rerun()

                    # Reset de contraseña (botón independiente)
                    if reset:
                        if not np1 or not np2:
                            st.warning("Ingresa y repite la nueva contraseña")
                        elif np1 != np2:
                            st.error("Las contraseñas no coinciden")
                        else:
                            ok, msg = set_user_password(uid, np1)
                            (st.success if ok else st.error)(msg)
                            if ok:
                                st.rerun()

                    # Eliminar usuario
                    if delete:
                        ok, msg = delete_user(uid)
                        (st.success if ok else st.error)(msg)
                        if ok:
                            st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)

    # (Fin Usuarios)
