# PlastiFlow - Control de Cajas

Aplicación Streamlit para gestionar viajes, despachos del Centro de Distribución (CD), devoluciones y envíos a Pastas Frescas.

## Arquitectura Actual

```
CajasPlasticas.py         # UI principal (Streamlit) - solo orquesta y llama servicios
app/
  config.py               # Configuración (path DB, etc.)
  db.py                   # Conexión y utilidades base SQLite
  models/                 # Modelos (fase de transición; viajes y CD migrados a services)
  services/
    stats_service.py      # Métricas dashboard y pendientes
    viajes_service.py     # Viajes + devoluciones + historial auditable
    cd_service.py         # Centro de Distribución
    locales_service.py    # CRUD de locales + helpers
    choferes_service.py   # Choferes: listar / crear / eliminar
    users_service.py      # Usuarios + roles + autenticación (hashing SHA-256)
                          # (Ahora usa bcrypt si está disponible, fallback sha256$)
```

### Principios
- La UI nunca toca directamente las tablas: usa funciones `svc_*`.
- Cada operación de escritura está aislada (facilita testing futuro).
- Validaciones críticas se hacen doble: UI (experiencia) + service (integridad).

### Roles
| Rol         | Permisos principales |
|-------------|----------------------|
| `admin`     | Total + acciones destructivas (eliminar, forzar, limpiar) |
| `cd_only`   | CRUD en CD y PF, sin gestión de usuarios |
| `no_cd_edit`| Solo lectura en secciones CD / PF |

## Flujo de Datos (simplificado)

Viajes:
1. Crear viaje -> inserta viaje + filas en `viaje_locales`.
2. Registrar devoluciones -> actualiza `viaje_locales` y agrega fila en `devoluciones_log`.
3. Historial editable -> CRUD sobre `devoluciones_log` + recalcula totales coherentes.

Centro de Distribución:
1. Crear despacho (cd_despachos) -> afecta stock global.
2. Registrar devoluciones CD -> suma devueltas y ajusta pendientes.
3. Envíos a origen (cd_envios_origen) -> reduce stock disponible.
4. Resúmenes -> agregaciones (pendientes por destino, totales stock).

## Próximas Fases Sugeridas
1. (Completado) Fase 4: Servicios locales, choferes y usuarios extraídos.
2. Fase 5: Índices SQLite (parcial: ya creados en `init_database`):
   - `CREATE INDEX IF NOT EXISTS idx_viajes_fecha ON viajes(fecha_viaje);`
   - `CREATE INDEX IF NOT EXISTS idx_devlog_created ON devoluciones_log(created_at);`
   - `CREATE INDEX IF NOT EXISTS idx_cd_despachos_fecha ON cd_despachos(fecha);`
   - `CREATE INDEX IF NOT EXISTS idx_cd_envios_fecha ON cd_envios_origen(fecha);`
3. Fase 6: Cache selectivo (`st.cache_data`) para catálogos (locales, choferes) y resúmenes; invalidar en escrituras.
4. Fase 7: Logging estructurado (JSON) + pruebas unitarias sobre capa services.
5. Fase 8: Página de auditoría (filtros por usuario, rango fechas sobre `devoluciones_log`).

## Próximos Pasos Detallados (Fase 4 en adelante)

### Fase 4: Servicios restantes (Completado)
- locales_service: CRUD de locales + normalización de display.
- choferes_service: listado y alta de choferes (futuro: baja lógica / auditoría).
- users_service: gestión de usuarios, cambio de contraseña, roles y verificación hash.

### Fase 5: Índices y performance
- Crear índices listados en sección anterior (ver Arquitectura) mediante script idempotente.
- Validar mejoras midiendo EXPLAIN QUERY PLAN (opcional).

### Fase 6: Cache selectiva
- Decorar funciones puras de lectura en stats_service y cd_service.
- Invalidar cache manualmente tras operaciones de escritura relevantes.

### Fase 7: Logging estructurado
- Añadir módulo `logging_conf.py` que configure logger JSON (nivel INFO).
- Loggear acciones: crear viaje, devoluciones (tipo, cantidad), despachos y envíos PF.

### Fase 8: Tests
- Pytest con fixture de DB en memoria.
- Casos mínimos: crear viaje, devoluciones masivas, revertir despacho, editar envío PF.

### Fase 9: Auditoría y exportación
- Página nueva: filtro por usuario / rango fecha sobre `devoluciones_log`.
- Export CSV del resultado filtrado.

## Estrategia de Cache (plan)
- Catalog data (locales, choferes): TTL 5-10 min.
- Resúmenes (stock CD, pendientes por destino): cacheados y se invalidan al crear/eliminar/actualizar despachos o envíos.
- Evitar cache en listados que el usuario edita inline inmediatamente.

## Testing (plan mínimo)
- Pytest sobre services: crear viaje, registrar devoluciones individuales/masivas, revertir y editar despacho, CRUD envíos PF.
- Usar DB temporal (`:memory:`) y factories simples.

## Hardening / Integridad
- Hash de contraseñas: bcrypt (prefijo `bcrypt$`); fallback legacy `sha256$` detectado.
- Foreign keys activados (`PRAGMA foreign_keys=ON`).
- Índices creados para queries frecuentes (fechas, joins de viajes y devoluciones).
- Validar que `cajas_devueltas <= cajas_enviadas` en todos los paths.
- Forzar tipos (int) y fechas normalizadas antes de persistir.
- Servicios devuelven `(ok: bool, msg: str | data)` para manejo consistente en UI.

## Cómo Ejecutar
1. Crear entorno virtual y instalar dependencias:
```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
2. Lanzar app:
```
streamlit run CajasPlasticas.py
```

## Mantenimiento Rápido
- Cuando se agregue nueva tabla: crear funciones CRUD en un service, no en la UI.
- Al modificar esquema: añadir migración ligera (ALTER) en `init_database` o script aparte.

## Ideas Futuras
- Exportaciones a CSV/Excel con filtros avanzados.
- Control de sesiones y auditoría de login.
- Alertas (p.ej. stock bajo) usando `st.toast` o correo.

---
Documentación generada automáticamente como base inicial. Ajustar según evolución del proyecto.
