import streamlit as st
import sqlite3
import pandas as pd
import datetime
from datetime import timedelta
import plotly.express as px
import plotly.graph_objects as go

# -------------------------------
# CONFIGURACIÓN DE LA APP
# -------------------------------
st.set_page_config(
    page_title="Control de Cajas por Chofer",
    page_icon="🚛",
    layout="wide"
)

# Estilos CSS personalizados mejorados
st.markdown("""
<style>
    /* Estilos generales */
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    
    /* Cards profesionales */
    .professional-card {
        background: white;
        border: 1px solid #e1e5e9;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        transition: all 0.3s ease;
    }
    
    .professional-card:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.12);
        transform: translateY(-2px);
    }
    
    /* Header personalizado */
    .custom-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem 1.5rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        text-align: center;
    }
    
    .custom-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 600;
    }
    
    .custom-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
        font-size: 1.1rem;
    }
    
    /* Sidebar mejorada */
    .css-1d391kg {
        background-color: #f8f9fa;
    }
    
    /* Botones profesionales */
    .stButton > button {
        border-radius: 8px;
        border: none;
        font-weight: 500;
        transition: all 0.3s ease;
        height: 2.5rem;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    
    /* Botón primario */
    .primary-button {
        background: linear-gradient(45deg, #667eea, #764ba2) !important;
        color: white !important;
        border: none !important;
        font-weight: 600 !important;
        padding: 0.75rem 2rem !important;
        border-radius: 8px !important;
    }
    
    /* Botón de peligro */
    .danger-button {
        background: linear-gradient(45deg, #ff416c, #ff4b2b) !important;
        color: white !important;
        border: none !important;
    }
    
    /* Botón de éxito */
    .success-button {
        background: linear-gradient(45deg, #56ab2f, #a8e6cf) !important;
        color: white !important;
        border: none !important;
    }
    
    /* Métricas del dashboard - NO TOCAR */
    .metric-card {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    
    /* Badges de estado */
    .status-badge {
        padding: 0.4rem 1rem;
        border-radius: 20px;
        font-size: 0.875rem;
        font-weight: 600;
        display: inline-block;
        margin: 0.25rem 0;
    }
    .status-activo { 
        background: linear-gradient(45deg, #4ade80, #22c55e); 
        color: white; 
    }
    .status-completado { 
        background: linear-gradient(45deg, #60a5fa, #3b82f6); 
        color: white; 
    }
    .status-pendiente { 
        background: linear-gradient(45deg, #fbbf24, #f59e0b); 
        color: white; 
    }
    
    /* Cards de viaje mejoradas */
    .viaje-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        transition: all 0.3s ease;
    }
    
    .viaje-card:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.1);
    }
    
    /* Formularios mejorados */
    .form-container {
        background: white;
        border: 1px solid #e1e5e9;
        border-radius: 12px;
        padding: 2rem;
        margin: 1rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    
    /* Tabs personalizadas */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: #f8f9fa;
        border-radius: 8px 8px 0px 0px;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(45deg, #667eea, #764ba2);
        color: white;
    }
    
    /* Info boxes */
    .info-box {
        background: #f0f9ff;
        border: 1px solid #0ea5e9;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .warning-box {
        background: #fffbeb;
        border: 1px solid #f59e0b;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .success-box {
        background: #f0fdf4;
        border: 1px solid #22c55e;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    /* Dividers personalizados */
    .custom-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, #667eea, transparent);
        border: none;
        margin: 2rem 0;
    }
    
    /* Data editor mejorado */
    .stDataEditor {
        border-radius: 8px;
        overflow: hidden;
    }
    
    /* Expanders mejorados */
    .streamlit-expanderHeader {
        background-color: #f8f9fa;
        border-radius: 8px;
    }
    
    /* Selectbox personalizado */
    .stSelectbox > div > div {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------------
# CONEXIÓN Y BASE DE DATOS
# -------------------------------
import os
def get_connection():
    # Detecta si está en Streamlit Cloud (ruta /mount/src/) o local
    base_dir = os.environ.get("STREAMLIT_CLOUD", None)
    if base_dir:
        db_path = os.path.join("/mount/src", "cajas_plasticas.db")
    else:
        # Usa el directorio actual del script
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cajas_plasticas.db")
    return sqlite3.connect(db_path, check_same_thread=False)

def init_database():
    conn = get_connection()
    c = conn.cursor()

    # Tabla choferes
    c.execute('''
        CREATE TABLE IF NOT EXISTS choferes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL UNIQUE,
            contacto TEXT,
            fecha_registro DATE DEFAULT CURRENT_DATE
        )
    ''')

    # Tabla viajes
    c.execute('''
        CREATE TABLE IF NOT EXISTS viajes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chofer_id INTEGER NOT NULL,
            fecha_viaje DATE NOT NULL,
            estado TEXT DEFAULT 'En Curso',
            FOREIGN KEY (chofer_id) REFERENCES choferes (id)
        )
    ''')

    # Tabla locales dentro de cada viaje
    c.execute('''
        CREATE TABLE IF NOT EXISTS viaje_locales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            viaje_id INTEGER NOT NULL,
            numero_local TEXT NOT NULL,
            cajas_enviadas INTEGER NOT NULL,
            cajas_devueltas INTEGER DEFAULT 0,
            FOREIGN KEY (viaje_id) REFERENCES viajes (id)
        )
    ''')

    # Tabla de locales general
    c.execute('''
        CREATE TABLE IF NOT EXISTS reception_local (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero INTEGER UNIQUE,
            nombre TEXT UNIQUE
        )
    ''')

    # Cargar locales iniciales si la tabla está vacía
    count = c.execute("SELECT COUNT(*) FROM reception_local").fetchone()[0]
    if count == 0:
        nombres = [
            "S6 Laurelty", "S6 Luque", "Stock Luque", "S6 Madero", "Stock Britez Borges", "Stock Cnel Martinez", "Stock Palma Loma", "Stock Limpio 1", "Stock Limpio 2", "Stock Villa Hayes", "S6 Portal", "Stock Mariano 1", "Stock Mariano 2", "CD EXPRESS (12)", "Stk Exp. Palma Loma", "Stk Exp. Britez Borges", "Stk Exp. Boqueron", "Stk Exp. Las Residentas", "Stk Exp. Primer Presidente", "S6 Exp. Moiety", "S6 Exp. Oviedo", "Stk Exp. Oviedo", "S6 Exp. San Bernardino 1", "S6 Exp. San Bernardino 2", "Stk Exp. Av. Paraguay", "Stk Exp. Sajonia 1", "Stk Exp. Sajonia 2", "Stk Exp. Patricio Esc", "Stk Exp. Tobati", "Stk Exp. San Isidro", "Stk Exp. Rca Colombia", "Stk Exp. 21 Proyectada", "S6 Exp. Yacht", "Stk Exp. Americo Picco", "Stk Exp. Campo Via", "Stk Exp. Figueroa", "Stk Exp. Las Mercedes", "Stk Exp. Peru", "Stk Exp. Tte Molas", "Stk Exp. 10 de Julio", "Stk Exp. San Lorenzo", "Stk Exp. Pratt Gill", "Stk Exp. Acosta Ñu", "Stk Exp. Campo Jordan", "Stk Exp. Pai Ñu", "Stk Exp. Panchito Lopez", "Stk Exp. Pinedo", "Stk Exp. La Victoria", "Stk Exp. Zeballos Kue", "Stk Exp. Bernardino Caballero", "Stk Exp. Nanawa", "Stk Exp. Piquetecue", "Stk Exp. Villa Hayes", "S6 Exp. Boquerón", "S6 Exp. Molas Lopez 1", "S6 Exp. San Martin", "Stk Exp. Avenida", "Stk Exp. Molas Lopez", "Stk Exp. Mompox", "Stk Exp. Av. Fernando", "Stk Exp. Soldado Ovelar", "Stk Exp. Pintiantuta", "Stk Exp. Guatambu", "Stk Exp. Sacramento", "Stk Exp. Brasilia", "Stk Exp. Gral Santos 2", "CARGA PRODUCTOS FRANZ", "S6 Aregua", "S6 San Bernardino", "S6 Carretera CDE", "S6 San Lorenzo", "Stock Basilica", "Stock Caacupe", "Stock Itaugua", "Stock Itaugua 2", "Stock Ita", "S6 Villeta", "Stock Acceso Sur", "Stock Boqueron", "Stock Defensores", "Stock Guarambare", "Stock San Antonio", "Stock Villa Elisa", "Stock Ypane", "S6 Ñemby", "S6 Japon", "S6 Lambare", "Stock Lambare", "Stock Lambare Carre.L", "Stock Mall Excelsior", "S6 Total", "S6 Sirio", "Stock Barrio Obrero", "Stock Doña Berta", "S6 Exp. Primer Presidente", "Stock C.D.E", "Stock Caacupe 2", "Stock Don Bosco", "Stock Hernandarias", "Stock J.A. Saldivar 1", "Stock J.A. Saldivar 2", "Stock San Lorenzo", "Stock Pdte. Franco", "Stock KM 8 Acaray", "S6 Encarnacion 1", "S6 Encarnacion 2", "Stock Cnel. Bogado", "Stock Paraguari", "Stock Carapegua 1", "Stock Carapegua 2", "Stock P.J.C", "Stock Santani", "S6 Villarrica", "Stock Caaguazu", "Stock Cnel. Oviedo", "Delimarket", "S6 Cambacua", "S6 Denis Roa", "S6 Galeria", "S6 Gran Union", "S6 Mburucuya", "Stock Avelino", "Stock Callei", "Stock Hiper San Lorenzo", "Stock Minga Guazu", "Stock Capiata 1", "Stock Capiata 2", "Stock La Victoria", "Stock Ortiz Guerrero", "Panaderia Centralizada", "S6 Fernando", "Stock Rca. Argentina", "Stock Unicompra", "S6 Hiper", "Stock Mcal Lopez", "Stock Fernando de la Mora", "S6 La Negrita", "S6 Los Laureles", "Stock Artigas", "Stock Brasilia 1", "Stock Brasilia 2", "S6 España", "S6 Mundimark", "Stock Sacramento", "S6 Villamorra", "CARGA PRODUCTOS FRANZ"
        ]
        for i, nombre in enumerate(nombres, 1):
            try:
                c.execute("INSERT INTO reception_local (numero, nombre) VALUES (?, ?)", (i, nombre))
            except:
                pass
        conn.commit()
    conn.close()

init_database()

# -------------------------------
# FUNCIONES DE CRUD
# -------------------------------
def agregar_chofer(nombre, contacto):
    conn = get_connection()
    try:
        conn.execute("INSERT INTO choferes (nombre, contacto) VALUES (?, ?)", (nombre, contacto))
        conn.commit()
        return True, "Chofer agregado exitosamente ✅"
    except sqlite3.IntegrityError:
        return False, f"El chofer '{nombre}' ya existe 🚫"
    finally:
        conn.close()

def get_choferes():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM choferes", conn)
    conn.close()
    return df

def eliminar_chofer(chofer_id):
    conn = get_connection()
    conn.execute("DELETE FROM choferes WHERE id = ?", (chofer_id,))
    conn.commit()
    conn.close()

def crear_viaje(chofer_id, fecha_viaje, locales):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO viajes (chofer_id, fecha_viaje) VALUES (?, ?)", (chofer_id, fecha_viaje))
    viaje_id = cur.lastrowid
    for local in locales:
        if local["numero_local"] and local["cajas_enviadas"] > 0:
            cur.execute(
                "INSERT INTO viaje_locales (viaje_id, numero_local, cajas_enviadas) VALUES (?, ?, ?)",
                (viaje_id, local["numero_local"], local["cajas_enviadas"])
            )
    conn.commit()
    conn.close()
    return viaje_id

def get_viajes():
    conn = get_connection()
    query = """
        SELECT v.id, v.fecha_viaje, v.estado, c.nombre as chofer_nombre
        FROM viajes v
        LEFT JOIN choferes c ON v.chofer_id = c.id
        ORDER BY v.fecha_viaje DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_viaje_locales(viaje_id):
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM viaje_locales WHERE viaje_id = ?", conn, params=(viaje_id,)
    )
    conn.close()
    return df

def get_dashboard_stats():
    conn = get_connection()
    
    stats = {}
    result = conn.execute("SELECT SUM(cajas_enviadas) FROM viaje_locales").fetchone()
    stats['total_enviadas'] = result[0] or 0
    result = conn.execute("SELECT SUM(cajas_devueltas) FROM viaje_locales").fetchone()
    stats['total_devueltas'] = result[0] or 0
    stats['pendientes'] = stats['total_enviadas'] - stats['total_devueltas']
    result = conn.execute("SELECT COUNT(*) FROM viajes WHERE estado = 'En Curso'").fetchone()
    stats['viajes_activos'] = result[0] or 0
    result = conn.execute("SELECT COUNT(*) FROM choferes").fetchone()
    stats['total_choferes'] = result[0] or 0
    
    conn.close()
    return stats

def get_cajas_por_chofer():
    conn = get_connection()
    query = """
        SELECT 
            c.nombre,
            SUM(vl.cajas_enviadas) as enviadas,
            SUM(vl.cajas_devueltas) as devueltas,
            SUM(vl.cajas_enviadas - vl.cajas_devueltas) as pendientes
        FROM choferes c
        LEFT JOIN viajes v ON c.id = v.chofer_id
        LEFT JOIN viaje_locales vl ON v.id = vl.viaje_id
        GROUP BY c.id, c.nombre
        HAVING enviadas > 0
        ORDER BY pendientes DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_viajes_detallados(start_date=None, end_date=None, chofer_id=None, estado=None):
    conn = get_connection()
    query = """
        SELECT 
            v.id,
            v.fecha_viaje,
            v.estado,
            c.nombre as chofer_nombre,
            COUNT(vl.id) as total_locales,
            SUM(vl.cajas_enviadas) as total_enviadas,
            SUM(vl.cajas_devueltas) as total_devueltas,
            SUM(vl.cajas_enviadas - vl.cajas_devueltas) as pendientes
        FROM viajes v
        LEFT JOIN choferes c ON v.chofer_id = c.id
        LEFT JOIN viaje_locales vl ON v.id = vl.viaje_id
        WHERE 1=1
    """
    params = []
    
    if start_date:
        query += " AND v.fecha_viaje >= ?"
        params.append(start_date)
    if end_date:
        query += " AND v.fecha_viaje <= ?"
        params.append(end_date)
    if chofer_id:
        query += " AND v.chofer_id = ?"
        params.append(chofer_id)
    if estado and estado != "Todos":
        query += " AND v.estado = ?"
        params.append(estado)
    
    query += " GROUP BY v.id ORDER BY v.fecha_viaje DESC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def registrar_devolucion(local_id, cantidad):
    conn = get_connection()
    conn.execute(
        "UPDATE viaje_locales SET cajas_devueltas = cajas_devueltas + ? WHERE id = ?",
        (cantidad, local_id)
    )
    conn.commit()
    conn.close()

def eliminar_viaje(viaje_id):
    conn = get_connection()
    conn.execute("DELETE FROM viaje_locales WHERE viaje_id = ?", (viaje_id,))
    conn.execute("DELETE FROM viajes WHERE id = ?", (viaje_id,))
    conn.commit()
    conn.close()

def actualizar_estado_viaje(viaje_id, nuevo_estado):
    conn = get_connection()
    conn.execute("UPDATE viajes SET estado = ? WHERE id = ?", (nuevo_estado, viaje_id))
    conn.commit()
    conn.close()

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
    st.markdown("### 📋 Menú Principal")
    menu = st.selectbox(
        "",
        ["🏠 Dashboard", "👷 Choferes", "🛣️ Viajes", "📥 Devoluciones", "🏪 Locales"],
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
    def agregar_local(numero, nombre):
        conn = get_connection()
        try:
            conn.execute("INSERT INTO reception_local (numero, nombre) VALUES (?, ?)", (numero, nombre))
            conn.commit()
            st.success("Local agregado correctamente.")
        except sqlite3.IntegrityError:
            st.error("Ya existe un local con ese número o nombre.")
        finally:
            conn.close()

    def editar_local(id, numero, nombre):
        conn = get_connection()
        try:
            conn.execute("UPDATE reception_local SET numero=?, nombre=? WHERE id=?", (numero, nombre, id))
            conn.commit()
            st.success("Local editado correctamente.")
        except sqlite3.IntegrityError:
            st.error("Ya existe un local con ese número o nombre.")
        finally:
            conn.close()

    def obtener_locales():
        conn = get_connection()
        locales = conn.execute("SELECT id, numero, nombre FROM reception_local ORDER BY numero").fetchall()
        conn.close()
        return locales

    st.header("Gestión de Locales")
    with st.form("agregar_local"):
        numero = st.number_input("Número de local", min_value=1, step=1)
        nombre = st.text_input("Nombre del local")
        submitted = st.form_submit_button("Agregar")
        if submitted:
            agregar_local(numero, nombre)

    st.subheader("Locales cargados")
    locales = obtener_locales()
    if locales:
        for id, numero, nombre in locales:
            st.write(f"**{numero}** - {nombre}")
            with st.expander("Editar", expanded=False):
                nuevo_numero = st.number_input(f"Editar número para {nombre}", min_value=1, value=numero, key=f"num_{id}")
                nuevo_nombre = st.text_input(f"Editar nombre para {nombre}", value=nombre, key=f"nom_{id}")
                if st.button("Guardar cambios", key=f"edit_{id}"):
                    editar_local(id, nuevo_numero, nuevo_nombre)
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
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Estado de Cajas")
        if stats['total_enviadas'] > 0:
            fig = go.Figure(data=[go.Pie(
                labels=['Devueltas', 'Pendientes'],
                values=[stats['total_devueltas'], stats['pendientes']],
                hole=.3,
                marker_colors=['#22c55e', '#f59e0b']
            )])
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos para mostrar")
    
    with col2:
        st.subheader("👷 Cajas por Chofer")
        cajas_chofer = get_cajas_por_chofer()
        if not cajas_chofer.empty:
            fig = px.bar(
                cajas_chofer, 
                x='nombre', 
                y=['enviadas', 'devueltas', 'pendientes'],
                title="Distribución por Chofer",
                barmode='group'
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos para mostrar")
    
    st.subheader("🔍 Detalle de Cajas Pendientes")
    if not cajas_chofer.empty:
        pendientes = cajas_chofer[cajas_chofer['pendientes'] > 0]
        if not pendientes.empty:
            st.dataframe(
                pendientes[['nombre', 'enviadas', 'devueltas', 'pendientes']], 
                use_container_width=True,
                column_config={
                    "nombre": "Chofer",
                    "enviadas": st.column_config.NumberColumn("📦 Enviadas"),
                    "devueltas": st.column_config.NumberColumn("✅ Devueltas"),
                    "pendientes": st.column_config.NumberColumn("⚠️ Pendientes")
                }
            )
        else:
            st.success("🎉 ¡Todas las cajas han sido devueltas!")
    else:
        st.info("No hay viajes registrados aún.")

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
                    ok, msg = agregar_chofer(nombre.strip(), contacto)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)

    st.markdown("### 📋 Lista de Choferes Registrados")
    choferes = get_choferes()
    
    if not choferes.empty:
        cols = st.columns(2)
        for idx, (_, row) in enumerate(choferes.iterrows()):
            with cols[idx % 2]:
                with st.container():
                    st.markdown(f"""
                    <div class="professional-card">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                            <h4 style="margin: 0; color: #1f2937;">👷 {row['nombre']}</h4>
                            <span style="background: #e5e7eb; padding: 0.25rem 0.5rem; border-radius: 12px; font-size: 0.75rem;">
                                ID: {row['id']}
                            </span>
                        </div>
                        <div style="color: #6b7280; margin-bottom: 1rem;">
                            <div>📞 {row['contacto'] or 'Sin contacto registrado'}</div>
                            <div>📅 Registrado: {row['fecha_registro']}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if st.button("🗑️ Eliminar", key=f"del_{row['id']}", type="secondary"):
                        eliminar_chofer(row['id'])
                        st.success("Chofer eliminado ✅")
                        st.rerun()
    else:
        st.markdown("""
        <div class="info-box">
            <h4>📝 No hay choferes registrados</h4>
            <p>Comienza agregando tu primer chofer usando el formulario de arriba.</p>
        </div>
        """, unsafe_allow_html=True)

# -------------------------------
# VIAJES
# -------------------------------
elif menu == "🛣️ Viajes":
    st.markdown("## 🛣️ Gestión de Viajes")
    st.markdown("Administra los viajes y el reparto de cajas")
    
    tab1, tab2 = st.tabs(["➕ Nuevo Viaje", "📋 Historial de Viajes"])
    
    with tab1:
        choferes = get_choferes()
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
                
                with st.form("nuevo_viaje", clear_on_submit=True):
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
                    
                    data = st.data_editor(
                        pd.DataFrame([
                            {"🏪 Local": "", "📦 Cajas": 0},
                            {"🏪 Local": "", "📦 Cajas": 0},
                            {"🏪 Local": "", "📦 Cajas": 0},
                            {"🏪 Local": "", "📦 Cajas": 0},
                            {"🏪 Local": "", "📦 Cajas": 0}
                        ]),
                        num_rows="dynamic",
                        use_container_width=True,
                        column_config={
                            "🏪 Local": st.column_config.TextColumn(
                                "Número/Nombre del Local", 
                                help="Identificador único del local",
                                required=True
                            ),
                            "📦 Cajas": st.column_config.NumberColumn(
                                "Cantidad de Cajas", 
                                min_value=0, 
                                step=1,
                                help="Número de cajas a entregar",
                                required=True
                            )
                        }
                    )

                    col1, col2, col3 = st.columns([1, 1, 1])
                    with col2:
                        submit = st.form_submit_button(
                            "🚀 Crear Viaje", 
                            use_container_width=True,
                            type="primary"
                        )
                    
                    if submit:
                        locales = []
                        for _, row in data.iterrows():
                            if row["🏪 Local"] and row["📦 Cajas"] > 0:
                                locales.append({
                                    "numero_local": row["🏪 Local"],
                                    "cajas_enviadas": row["📦 Cajas"]
                                })
                        
                        if locales:
                            viaje_id = crear_viaje(chofer_id, fecha, locales)
                            st.success(f"✅ Viaje #{viaje_id} creado exitosamente")
                            st.balloons()
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
                fecha_inicio = st.date_input("📅 Fecha Inicio", 
                                           value=datetime.date.today() - timedelta(days=30),
                                           key="viajes_fecha_inicio")
            with col2:
                fecha_fin = st.date_input("📅 Fecha Fin", 
                                        value=datetime.date.today(),
                                        key="viajes_fecha_fin")
            with col3:
                choferes = get_choferes()
                chofer_options = pd.concat([pd.DataFrame({"id": [None], "nombre": ["Todos"]}), choferes[["id", "nombre"]]])
                chofer_id = st.selectbox(
                    "👷 Chofer",
                    chofer_options["id"],
                    format_func=lambda x: "Todos" if x is None else chofer_options[chofer_options["id"] == x]["nombre"].iloc[0],
                    key="viajes_chofer"
                )
            
            estado = st.selectbox(
                "📊 Estado",
                ["Todos", "En Curso", "Completado"],
                key="viajes_estado"
            )
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        viajes = get_viajes_detallados(
            start_date=fecha_inicio,
            end_date=fecha_fin,
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
                        st.markdown(f"**👷 Chofer:** {row['chofer_nombre']}")
                        st.markdown(f"**📅 Fecha:** {row['fecha_viaje']}")
                    
                    with col2:
                        estado_color = "status-activo" if row['estado'] == "En Curso" else "status-completado"
                        st.markdown(
                            f'<div style="text-align: center;"><span class="status-badge {estado_color}">{row["estado"]}</span></div>', 
                            unsafe_allow_html=True
                        )
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
                        locales = get_viaje_locales(row['id'])
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
                                    actualizar_estado_viaje(row['id'], 'Completado')
                                    st.success("✅ Estado actualizado a Completado")
                                    st.rerun()
                        
                        with col_btn2:
                            if row['estado'] == 'Completado':
                                if st.button("🔄 Reactivar Viaje", key=f"reactivate_{row['id']}", type="secondary"):
                                    actualizar_estado_viaje(row['id'], 'En Curso')
                                    st.success("🔄 Viaje reactivado")
                                    st.rerun()
                        
                        with col_btn3:
                            if st.button("🗑️ Eliminar Viaje", key=f"del_viaje_{row['id']}", type="secondary"):
                                eliminar_viaje(row['id'])
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
# DEVOLUCIONES
# -------------------------------
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
            choferes = get_choferes()
            chofer_options = pd.concat([pd.DataFrame({"id": [None], "nombre": ["Todos"]}), choferes[["id", "nombre"]]])
            chofer_id = st.selectbox(
                "👷 Chofer",
                chofer_options["id"],
                format_func=lambda x: "Todos" if x is None else chofer_options[chofer_options["id"] == x]["nombre"].iloc[0],
                key="devoluciones_chofer"
            )
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    viajes = get_viajes_detallados(
        start_date=fecha_inicio,
        end_date=fecha_fin,
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
                format_func=lambda x: f"🚛 Viaje #{x} - {viajes[viajes['id']==x]['chofer_nombre'].iloc[0]} - {viajes[viajes['id']==x]['fecha_viaje'].iloc[0]}",
                label_visibility="collapsed"
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        locales = get_viaje_locales(viaje_id)

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
            st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)
            
            cols = st.columns(2)
            for idx, (_, row) in enumerate(locales.iterrows()):
                pendientes = row["cajas_enviadas"] - row["cajas_devueltas"]
                
                with cols[idx % 2]:
                    with st.container():
                        if pendientes > 0:
                            st.markdown(f"""
                            <div class="professional-card" style="border-left: 4px solid #f59e0b;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                                    <h4 style="margin: 0; color: #1f2937;">🏪 {row['numero_local']}</h4>
                                    <span class="status-badge status-pendiente">⚠️ Pendiente</span>
                                </div>
                                <div style="color: #6b7280; margin-bottom: 1rem;">
                                    <div style="display: flex; justify-content: space-between;">
                                        <span>📦 Enviadas:</span><strong>{row['cajas_enviadas']}</strong>
                                    </div>
                                    <div style="display: flex; justify-content: space-between;">
                                        <span>✅ Devueltas:</span><strong>{row['cajas_devueltas']}</strong>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; color: #f59e0b;">
                                        <span>⚠️ Pendientes:</span><strong>{pendientes}</strong>
                                    </div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div class="professional-card" style="border-left: 4px solid #22c55e;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                                    <h4 style="margin: 0; color: #1f2937;">🏪 {row['numero_local']}</h4>
                                    <span class="status-badge status-activo">🎉 Completo</span>
                                </div>
                                <div style="color: #6b7280; margin-bottom: 1rem;">
                                    <div style="display: flex; justify-content: space-between;">
                                        <span>📦 Enviadas:</span><strong>{row['cajas_enviadas']}</strong>
                                    </div>
                                    <div style="display: flex; justify-content: space-between;">
                                        <span>✅ Devueltas:</span><strong>{row['cajas_devueltas']}</strong>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; color: #22c55e;">
                                        <span>🎉 Estado:</span><strong>Completo</strong>
                                    </div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                        
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
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    if st.form_submit_button("📥 Registrar", use_container_width=True, type="primary"):
                                        if cantidad > 0:
                                            registrar_devolucion(row['id'], cantidad)
                                            st.success(f"✅ {cantidad} cajas registradas para {row['numero_local']}")
                                            st.rerun()
                                        else:
                                            st.warning("⚠️ Ingresa una cantidad mayor a 0")
                                
                                with col2:
                                    if st.form_submit_button("📥 Devolver Todas", use_container_width=True):
                                        registrar_devolucion(row['id'], pendientes)
                                        st.success(f"✅ Todas las cajas ({pendientes}) registradas para {row['numero_local']}")
                                        st.rerun()
                        
                        st.markdown('<div style="margin-bottom: 1rem;"></div>', unsafe_allow_html=True)
            
            if total_pendientes == 0:
                st.markdown("""
                <div class="success-box">
                    <h4>🎉 ¡Viaje Completado!</h4>
                    <p>Todas las cajas han sido devueltas. ¿Te gustaría marcar este viaje como completado?</p>
                </div>
                """, unsafe_allow_html=True)
                
                if st.button("✅ Marcar Viaje como Completado", type="primary"):
                    actualizar_estado_viaje(viaje_id, 'Completado')
                    st.success("🎉 Viaje marcado como completado")
                    st.balloons()
                    st.rerun()
