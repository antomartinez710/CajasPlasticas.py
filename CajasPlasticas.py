import streamlit as st
import sqlite3
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime
from datetime import timedelta

# Configuración de página
st.set_page_config(
    page_title="Pastas Frescas — Control de Cajas",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

BRAND = {
    "name": "Pastas Frescas",
    "primary": "#1e3a8a",   # azul profundo
    "accent": "#b91c1c",    # rojo
    "success": "#059669",   # verde
    "warn": "#b45309",      # naranja
    "muted": "#374151",     # gris texto
}

# -------------------------------
# ESTILOS GLOBALES
# -------------------------------
st.markdown("""
<style>
/* Header limpio */
.app-header {
  background: white;
  border-bottom: 1px solid #e5e7eb;
  padding: 1rem 0.5rem 0.75rem 0.5rem;
  margin-bottom: 0.75rem;
}
.app-title {
  font-size: 1.4rem;
  font-weight: 600;
  color: #111827;
}
.app-subtitle {
  color: #6b7280;
  font-size: 0.95rem;
}

/* Badges de estado (más sobrios) */
.status-badge {
  padding: 0.25rem 0.6rem;
  border-radius: 999px;
  font-size: 0.8rem;
  font-weight: 600;
  display: inline-block;
}
.status-activo { background: #ecfdf5; color: #059669; border: 1px solid #a7f3d0; }
.status-completado { background: #eff6ff; color: #1d4ed8; border: 1px solid #bfdbfe; }
.status-pendiente { background: #fffbeb; color: #b45309; border: 1px solid #fde68a; }

/* Cards y formularios */
.viaje-card, .professional-card, .form-container {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 1rem 1.25rem;
  box-shadow: 0 1px 2px rgba(0,0,0,0.03);
}
.viaje-card { margin: 0.75rem 0; }
.professional-card { margin: 0.5rem 0; }

/* Tabs limpias */
.stTabs [data-baseweb="tab-list"] { gap: 6px; }
.stTabs [data-baseweb="tab"] {
  height: 42px;
  padding: 0 14px;
  background-color: #f9fafb;
  border-radius: 6px 6px 0 0;
  font-weight: 500;
  color: #374151;
}
.stTabs [aria-selected="true"] {
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-bottom-color: #ffffff;
  color: #111827;
}

/* Alertas/Info más discretas */
.info-box { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 0.9rem; }
.warning-box { background: #fffbeb; border: 1px solid #f59e0b; border-radius: 8px; padding: 0.9rem; }
.success-box { background: #f0fdf4; border: 1px solid #22c55e; border-radius: 8px; padding: 0.9rem; }

/* Divisor sutil */
.custom-divider { height: 1px; background: #e5e7eb; border: none; margin: 1.2rem 0; }

/* Pequeños redondeos */
.stDataEditor { border-radius: 8px; overflow: hidden; }
.streamlit-expanderHeader { background-color: #f9fafb; border-radius: 8px; }
.stSelectbox > div > div { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# -------------------------------
# CONEXIÓN Y BASE DE DATOS
# -------------------------------
def get_db_path():
    """Devuelve la ruta absoluta del archivo SQLite, tanto local como en Streamlit Cloud."""
    base_dir = os.environ.get("STREAMLIT_CLOUD", None)
    if base_dir:
        return os.path.join("/mount/src", "cajas_plasticas.db")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "cajas_plasticas.db")

def get_connection():
    # Detecta si está en Streamlit Cloud (ruta /mount/src/) o local
    base_dir = os.environ.get("STREAMLIT_CLOUD", None)
    if base_dir:
        db_path = os.path.join("/mount/src", "cajas_plasticas.db")
    else:
        # Usa el directorio actual del script
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cajas_plasticas.db")
    
    try:
        # Intenta crear el directorio si no existe
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        # Verifica que la conexión funciona
        conn.execute("SELECT 1").fetchone()
        return conn
    except Exception as e:
        st.error(f"Error al conectar con la base de datos: {str(e)}")
        st.error(f"Ruta de la base de datos: {db_path}")
        raise

def init_database():
    try:
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
                except sqlite3.IntegrityError:
                    # Ignora duplicados
                    pass
            conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Error al inicializar la base de datos: {str(e)}")
        st.error("La aplicación podría no funcionar correctamente. Por favor, verifica los permisos de escritura.")
        # No relanzar la excepción para que la aplicación pueda funcionar parcialmente

# Inicializar la base de datos con manejo de errores
try:
    init_database()
except Exception as e:
    st.error("Error crítico al inicializar la base de datos. La aplicación podría no funcionar correctamente.")
    st.error(f"Detalles del error: {str(e)}")

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

def get_locales_catalogo():
    """Devuelve una lista de diccionarios con numero, nombre y display 'numero - nombre'."""
    conn = None
    try:
        conn = get_connection()
        rows = conn.execute("SELECT numero, nombre FROM reception_local ORDER BY numero").fetchall()
        return [
            {"numero": r[0], "nombre": r[1], "display": f"{r[0]} - {r[1]}"}
            for r in rows
        ]
    except Exception:
        return []
    finally:
        if conn:
            conn.close()

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

def registrar_devolucion_todas_por_viaje(viaje_id):
    """Marca como devueltas todas las cajas pendientes de un viaje."""
    conn = get_connection()
    try:
        # Aumenta cajas_devueltas a cajas_enviadas para todos los locales del viaje
        conn.execute(
            """
            UPDATE viaje_locales
            SET cajas_devueltas = cajas_enviadas
            WHERE viaje_id = ? AND cajas_devueltas < cajas_enviadas
            """,
            (viaje_id,)
        )
        conn.commit()
    finally:
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
        st.markdown(f"<div style='font-weight:700;color:#111827;'> {BRAND['name']}</div>", unsafe_allow_html=True)
        st.caption("Control de Cajas")

    st.markdown("---")
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
    def numero_existe(numero, exclude_id=None):
        """Devuelve True si el número ya está asignado a otro local.
        Si exclude_id está definido, ignora ese registro (útil al editar).
        """
        conn = None
        try:
            conn = get_connection()
            if exclude_id is None:
                row = conn.execute(
                    "SELECT 1 FROM reception_local WHERE numero = ? LIMIT 1",
                    (numero,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT 1 FROM reception_local WHERE numero = ? AND id <> ? LIMIT 1",
                    (numero, exclude_id)
                ).fetchone()
            return row is not None
        except Exception:
            return False
        finally:
            if conn:
                conn.close()

    def siguiente_numero_disponible():
        """Obtiene el próximo número sugerido (MAX(numero)+1) o 1 si no hay registros."""
        conn = None
        try:
            conn = get_connection()
            row = conn.execute("SELECT MAX(numero) FROM reception_local").fetchone()
            max_num = row[0] if row and row[0] is not None else 0
            return int(max_num) + 1
        except Exception:
            return 1
        finally:
            if conn:
                conn.close()

    def agregar_local(numero, nombre):
        conn = None
        try:
            # Validación de duplicados por número antes de insertar
            if numero_existe(numero):
                st.error("Ya existe un local con ese número.")
                return
            conn = get_connection()
            conn.execute("INSERT INTO reception_local (numero, nombre) VALUES (?, ?)", (numero, nombre))
            conn.commit()
            st.success("Local agregado correctamente.")
        except sqlite3.IntegrityError:
            st.error("Ya existe un local con ese número o nombre.")
        except sqlite3.OperationalError as e:
            st.error(f"Error de base de datos: {str(e)}")
            st.error("Por favor, verifica que la base de datos esté accesible.")
        except Exception as e:
            st.error(f"Error inesperado: {str(e)}")
        finally:
            if conn:
                conn.close()

    def editar_local(id, numero, nombre):
        conn = None
        try:
            # Validación de duplicados por número, excluyendo el propio registro
            if numero_existe(numero, exclude_id=id):
                st.error("No se puede asignar un número que ya está usado por otro local.")
                return
            conn = get_connection()
            conn.execute("UPDATE reception_local SET numero=?, nombre=? WHERE id=?", (numero, nombre, id))
            conn.commit()
            st.success("Local editado correctamente.")
        except sqlite3.IntegrityError:
            st.error("Ya existe un local con ese número o nombre.")
        except sqlite3.OperationalError as e:
            st.error(f"Error de base de datos: {str(e)}")
            st.error("Por favor, verifica que la base de datos esté accesible.")
        except Exception as e:
            st.error(f"Error inesperado: {str(e)}")
        finally:
            if conn:
                conn.close()

    def eliminar_local(id):
        conn = None
        try:
            conn = get_connection()
            conn.execute("DELETE FROM reception_local WHERE id=?", (id,))
            conn.commit()
            st.success("Local eliminado correctamente.")
        except sqlite3.OperationalError as e:
            st.error(f"Error de base de datos: {str(e)}")
            st.error("Por favor, verifica que la base de datos esté accesible.")
        except Exception as e:
            st.error(f"Error inesperado: {str(e)}")
        finally:
            if conn:
                conn.close()

    def obtener_locales():
        conn = None
        try:
            conn = get_connection()
            locales = conn.execute("SELECT id, numero, nombre FROM reception_local ORDER BY numero").fetchall()
            return locales
        except sqlite3.OperationalError as e:
            st.error(f"Error de base de datos: {str(e)}")
            st.error("Por favor, verifica que la base de datos esté accesible.")
            return []
        except Exception as e:
            st.error(f"Error inesperado al obtener locales: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    st.header("Gestión de Locales")
    with st.form("agregar_local"):
        numero = st.number_input("Número de local", min_value=1, step=1, value=siguiente_numero_disponible())
        nombre = st.text_input("Nombre del local")
        submitted = st.form_submit_button("Agregar")
        if submitted:
            agregar_local(numero, nombre)

    st.subheader("Locales cargados")
    locales = obtener_locales()
    if locales:
        # Crear DataFrame base
        import pandas as pd
        df_locales = pd.DataFrame(locales, columns=['ID', 'Número', 'Nombre'])

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
                            editar_local(id_local, nuevo_numero, nuevo_nombre)
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
                        eliminar_local(id_local)
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
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Estado de Cajas")
        if stats['total_enviadas'] > 0:
            fig = go.Figure(data=[go.Pie(
                labels=['Devueltas', 'Pendientes'],
                values=[stats['total_devueltas'], stats['pendientes']],
                hole=.3,
                marker_colors=[BRAND['success'], BRAND['warn']]
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
                barmode='group',
                color_discrete_sequence=[BRAND['primary'], BRAND['success'], BRAND['warn']]
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

    # Herramienta puntual para limpiar datos de prueba (temporal)
    with st.expander("🧹 Limpiar datos de prueba", expanded=False):
        st.caption("Elimina viajes y sus locales asociados. Mantiene la lista de Locales. Opcional: eliminar Choferes.")
        # Calcular conteos
        try:
            conn = get_connection()
            n_viajes = conn.execute("SELECT COUNT(*) FROM viajes").fetchone()[0]
            n_vl = conn.execute("SELECT COUNT(*) FROM viaje_locales").fetchone()[0]
            n_ch = conn.execute("SELECT COUNT(*) FROM choferes").fetchone()[0]
            conn.close()
        except Exception:
            n_viajes = n_vl = n_ch = 0

        st.write(f"Viajes: {n_viajes} · Ítems de viaje_locales: {n_vl} · Choferes: {n_ch}")
        del_ch = st.checkbox("También eliminar Choferes", value=False)
        confirm = st.checkbox("Entiendo que esto es irreversible", value=False, key="purge_confirm_dashboard")
        if st.button("🗑️ Borrar datos de prueba", type="secondary", disabled=not confirm):
            try:
                conn = get_connection()
                conn.execute("DELETE FROM viaje_locales")
                conn.execute("DELETE FROM viajes")
                if del_ch:
                    conn.execute("DELETE FROM choferes")
                conn.commit()
                conn.close()
                st.success("Datos eliminados. Los Locales se mantuvieron intactos.")
                st.balloons()
                st.rerun()
            except Exception as e:
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
                opciones_locales = [placeholder_local] + ([item["display"] for item in catalogo_locales] if catalogo_locales else [])

                # Modo por ítems: menos reruns, cada fila se agrega con un submit explícito
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
                    if local_sel != placeholder_local and cajas_item > 0:
                        st.session_state["nuevo_viaje_items"].append({
                            "🏪 Local": local_sel,
                            "📦 Cajas": int(cajas_item)
                        })
                    else:
                        st.warning("Selecciona un local y una cantidad mayor a 0")

                # Listado de ítems agregados con botón de eliminar en la misma fila
                items = st.session_state["nuevo_viaje_items"]
                if not items:
                    st.info("No hay ítems agregados todavía.")
                else:
                    # Encabezados
                    hc1, hc2, hc3 = st.columns([6, 2, 1])
                    with hc1:
                        st.markdown("**🏪 Local**")
                    with hc2:
                        st.markdown("**📦 Cajas**")
                    with hc3:
                        st.markdown("**Acciones**")

                    # Filas
                    for idx, it in enumerate(items):
                        rc1, rc2, rc3 = st.columns([6, 2, 1])
                        with rc1:
                            st.text(str(it.get("🏪 Local", "")))
                        with rc2:
                            st.text(str(it.get("📦 Cajas", "")))
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
                        {"numero_local": it["🏪 Local"], "cajas_enviadas": int(it["📦 Cajas"])}
                        for it in st.session_state["nuevo_viaje_items"]
                        if it.get("🏪 Local") and (it.get("📦 Cajas") or 0) > 0
                    ]
                    if locales:
                        viaje_id = crear_viaje(chofer_id, fecha, locales)
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
                            registrar_devolucion_todas_por_viaje(viaje_id)
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
                            actualizar_estado_viaje(viaje_id, 'Completado')
                            try:
                                del st.session_state["finalize_prompt_viaje_id"]
                            except KeyError:
                                pass
                            st.success("🚛 Viaje marcado como Completado")
                            st.rerun()
                    with c2:
                        if st.button("⏳ Ahora no", key=f"no_finalizar_{viaje_id}", use_container_width=True):
                            try:
                                del st.session_state["finalize_prompt_viaje_id"]
                            except KeyError:
                                pass
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
            
            if total_pendientes == 0 and st.session_state.get("finalize_prompt_viaje_id") != viaje_id:
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

# (Sección de Administración eliminada a pedido del usuario)
