# INDEX — Brand Benchmark Suite (BBS)

Mapa completo de carpetas y archivos del proyecto. Se actualiza en automático — ver protocolo en [CLAUDE.md](CLAUDE.md).

**Última actualización:** 2026-08-31

**Excluidos de este índice:** `node_modules/`, `.next/`, `.venv/`, `__pycache__/`, `.pytest_cache/`,
`*.pyc`, `apps/web/tsconfig.tsbuildinfo`, archivos `.env*`. La carpeta **`data/`** completa está
excluida de git (`.gitignore`) y por tanto de este índice de código — es el warehouse local
(parquet/DuckDB + `.sav` fuente); ver la sección "Datos locales" en [CLAUDE.md](CLAUDE.md) para su
estructura.

---

## Raíz
| Archivo | Descripción |
|---|---|
| [README.md](README.md) | Qué es el proyecto, stack, setup local, estado y comportamiento funcional por página. |
| [CLAUDE.md](CLAUDE.md) | Instrucciones para Claude Code: comandos, arquitectura, protocolo de este sistema. |
| [INDEX.md](INDEX.md) | Este archivo. |
| [BITACORA.md](BITACORA.md) | Registro cronológico de trabajo. |
| [LICENSE](LICENSE) | Licencia del repositorio. |
| [.editorconfig](.editorconfig) | Reglas de formato de editor (indentación, EOL). |
| [.gitattributes](.gitattributes) | Normalización de line endings en git. |
| [.gitignore](.gitignore) | Excluye `node_modules`, `.next`, `.venv`, `data/`, `*.parquet`, `.env*`, contrato SQL borrador. |

## [apps/web/](apps/web/) — Frontend Next.js
| Archivo | Descripción |
|---|---|
| package.json | Scripts (`dev`/`build`/`lint`/`start`) y dependencias (Next 14, ECharts, Radix, Supabase JS, xlsx). |
| next.config.js | Rewrites `/analytics/*`, `/filters/*`, `/network` → `/api/*` (desacopla frontend del data source activo). |
| tsconfig.json | TS strict mode, paths de Next. |
| tailwind.config.js / postcss.config.js | Configuración de Tailwind. |
| README.md | Notas propias del paquete web (setup rápido). |

### apps/web/src/app — páginas (App Router)
| Archivo | Descripción |
|---|---|
| layout.tsx | Layout raíz (envuelve `AppShell`). |
| page.tsx | Redirige `/` → `/journey`. |
| globals.css | Estilos globales Tailwind. |
| journey/page.tsx | Página Journey (funnel, benchmarks, heatmap). |
| demand-network/page.tsx | Página Network (grafo de touchpoints/demanda). |
| tracking/page.tsx | Página Tracking (comparación por periodo). |
| admin/page.tsx, admin/components/DataOpsPanel.tsx | Panel Admin — operaciones de datos (upload/jobs/publish), legado del módulo `/data`. |
| data/page.tsx, data/components/DataOpsPanel.tsx | Página `/data` — UI operativa de ingesta: sube `.sav`, corre jobs, publica versiones a Supabase. |
| agent/page.tsx | UI de chat del Agente BBS. |
| auth/page.tsx | Formulario de login (magic link). |
| auth/reset/page.tsx | Reseteo de contraseña/sesión. |
| auth/callback/route.ts | Intercambio de código de sesión de Supabase. |

### apps/web/src/app/api — gateway interno (Next API routes)
Todo el frontend pasa por aquí; nunca se llama a FastAPI o Supabase directo desde componentes cliente.

| Archivo | Descripción |
|---|---|
| `[...path]/route.ts` | Forwarder catch-all a FastAPI (solo modo `legacy`; 404 en modo `supabase`). |
| `_lib/backend.ts` | `getDataSource()` (`legacy`\|`supabase`), `forwardLegacy`, `callSupabaseRpc`, `handleWithDataSource`. |
| `_lib/authz.ts` | `getRequestAuthz` — resuelve rol/permisos/scopes del usuario; `isMutatingDataPath`. |
| `_lib/access-scope.ts` | `getScopeContext`/`scopeStudyIds` — intersecta `study_id`s permitidos para rol `viewer`. |
| `_lib/market-lens.ts` | Deriva/resuelve el "market lens" (sector/subsector/categoría normalizados) de una request. |
| `_lib/market-filter-scope.ts` | Aplica el market lens como filtro de `study_id`s contra el catálogo. |
| `_lib/demographics.ts` | `expandNseInQuery`/`expandNseInPayload` — expande grupos NSE (AB/C/DE) a valores crudos. |
| `_lib/supabase-admin.ts` | Cliente Postgrest/Auth admin de Supabase (service role) para rutas admin/data. |
| `analytics/journey/table_multi/route.ts` | Gateway Journey — aplica scope, market lens, NSE, delega a `handleWithDataSource`. |
| `analytics/touchpoints/table_multi/route.ts` | Gateway Network/touchpoints (mismo patrón). |
| `analytics/tracking/series/route.ts` | Gateway Tracking (mismo patrón). |
| `network/route.ts` | Gateway del grafo `/network`. |
| `filters/options/{studies,taxonomy,demographics,date}/route.ts` | Opciones para la Scope Bar. |
| `auth/me/route.ts` | Devuelve el `authz` resuelto del usuario actual (rol, permisos, scopes). |
| `admin/users/route.ts` | Lista/crea usuarios (Supabase Auth admin). |
| `admin/users/[userId]/route.ts` | Actualiza rol de un usuario. |
| `admin/users/[userId]/access/route.ts` | Actualiza scopes de acceso (sector/subsector/categoría) de un usuario. |
| `data/upload/route.ts` | Sube un archivo `.sav` a `uploaded_files` (Supabase) para ingesta. |
| `data/files/route.ts` | Lista `uploaded_files` recientes. |
| `data/jobs/route.ts` | Lista `ingestion_jobs` recientes. |
| `data/jobs/[jobId]/run/route.ts` | Ejecuta un job de ingesta y escribe `ingestion_job_logs`. |
| `data/studies/route.ts` | Lista estudios con su estado de pipeline (raw/mapping/curated/publicado). |
| `data/studies/[studyId]/delete/route.ts` | Elimina un estudio (rol `owner`/`admin`). |
| `data/versions/route.ts` | Lista `data_versions`. |
| `data/versions/[versionId]/publish/route.ts` | Publica una versión de datos. |
| `data/push/route.ts` | Empuja snapshot local (journey/touchpoints/taxonomía/demografía) a Supabase. |
| `data/publish/history/route.ts` | Historial de operaciones `push_snapshot`. |

### apps/web/src/app/api/agent — módulo Agente
| Archivo | Descripción |
|---|---|
| AGENTE.md | **Fuente única de comportamiento/persona del agente** (rol, estilo de respuesta, control de acceso, mensajes canónicos en español). Editar aquí, no duplicar reglas en el código. |
| `_lib/agent.ts` | Pipeline de generación de respuesta (`generateAgentResponse`), control de acceso (`requireAgentAccess`), idioma de respuesta es/en, gestión de conversaciones. |
| `conversations/route.ts` | Lista/crea conversaciones del usuario. |
| `conversations/[conversationId]/route.ts` | Obtiene/borra una conversación (valida ownership). |
| `conversations/[conversationId]/messages/route.ts` | Envía un mensaje y obtiene la respuesta del agente. |

### apps/web/src/components — UI compartida
| Archivo | Descripción |
|---|---|
| FilterBar.tsx | Barra de filtros (legado/genérica). |
| JourneyChart.tsx | Gráfico ECharts de Journey. |
| NetworkCanvas.tsx | Canvas del grafo de Network. |
| StudySelector.tsx | Selector de estudio(s). |
| layout/AppShell.tsx | Shell de la app: monta `TopNav` + `ScopeBar` + `ScopeProvider`, oculta chrome en `/auth`. |
| layout/TopNav.tsx | Navegación superior; oculta ítems según `authz.role`/`is_admin_module_allowed`. |
| layout/ScopeBar.tsx | Barra de filtros globales (Sector/Subsector/Categoría, Brands, demo, años). |
| layout/ScopeProvider.tsx | Contexto React de la Scope Bar; carga opciones y ejecuta queries de Journey/Touchpoints. |
| demand-network/ControlsToolbar.tsx | Controles de Network (métrica, layout, distancia). |
| demand-network/graphUtils.ts | Utilidades de layout/cálculo del grafo. |
| demand-network/views/DemandNetworkView.tsx | Vista principal del grafo de demanda. |
| demand-network/views/MatrixView.tsx | Vista de matriz touchpoint × brand. |
| demand-network/views/SankeyView.tsx | Vista Sankey del flujo de demanda. |
| demand-network/views/SmallMultiplesView.tsx | Vista de pequeños múltiplos por segmento. |
| demand-network/views/TimeView.tsx | Vista temporal del grafo. |
| demand-network/views/helpers.ts, types.ts | Helpers y tipos compartidos de las vistas de Network. |

### apps/web/src/features — lógica específica de página
| Archivo | Descripción |
|---|---|
| journey/components/*.tsx | `FocusBar`, `HeroSankey`, `JourneyDataValidationTables`, `JourneyHeatmapTable`, `JourneyInsights`, `JourneyKpiStrip`, `TimeScrubber` — piezas de UI de la página Journey. |
| journey/data/README.md | Notas sobre el modelo de datos de Journey. |
| journey/data/journeySchema.ts | Tipos/schema de la respuesta de Journey. |
| journey/data/journeyDerived.ts | Cálculos derivados (deltas, índices) sobre datos de Journey. |
| journey/data/journeySelectors.ts | Selectores memoizados sobre el estado de Journey. |
| journey/data/journeyTransforms.ts | Transformaciones de la respuesta cruda a modelos de UI. |
| journey/data/journeyDataSanity.ts | Validaciones de sanidad de datos antes de render. |
| journey/heatmap/buildJourneyHeatmap.ts | Construye la matriz del heatmap (benchmarks + marcas). |
| journey/insights/generateJourneyInsights.ts | Genera insights textuales a partir del modelo Journey. |
| tracking/components/TrackingCharts.tsx | Gráficos de series de Tracking. |
| tracking/components/TrackingComparisonTable.tsx | Tabla de comparación por periodo. |
| tracking/components/TrackingKpiStrip.tsx | Tira de KPIs de Tracking. |
| tracking/components/TrackingStudyPicker.tsx | Selector de estudios/periodos para Tracking. |
| tracking/data/buildTrackingSeriesModel.ts | Construye el modelo de series a partir de la respuesta de la API. |
| tracking/export/exportTrackingXlsx.ts | Exporta la comparación visible a Excel. |
| tracking/types.ts | Tipos compartidos del feature Tracking. |

### apps/web/src/lib, utils, middleware
| Archivo | Descripción |
|---|---|
| lib/api.ts | Cliente tipado del gateway interno `/api/*` (todas las llamadas fetch del frontend). |
| lib/types.ts | Tipos compartidos de dominio (Journey/Network/Tracking). |
| lib/supabase/config.ts | `isSupabaseAuthEnabled()` y config de Supabase para el cliente. |
| lib/supabase/browser.ts | Cliente Supabase de navegador. |
| utils/supabase/client.ts, server.ts, middleware.ts | Clientes Supabase por contexto (browser/server/middleware), vía `@supabase/ssr`. |
| middleware.ts | Gate de auth/rol/scope de páginas y `/api/*` (ver detalle en CLAUDE.md). |

## [services/api/](services/api/) — Backend FastAPI
| Archivo | Descripción |
|---|---|
| README.md | Notas propias del servicio. |
| requirements.txt | Dependencias (FastAPI, DuckDB, pandas, pyarrow, pyreadstat). |
| .env.example | Variables: `API_TITLE`, `API_VERSION`, `CORS_ORIGINS`. |
| app/main.py | Crea la app FastAPI, registra todos los routers y CORS. |
| app/core/config.py | `Settings` desde variables de entorno, incluye `rate_model_source` (`BBS_RATE_MODEL_SOURCE`: `auto`\|`sql`\|`parquet`). |
| app/core/logging.py | Configuración básica de logging. |
| app/models/schemas.py | Modelos Pydantic (`Study`, etc.). |
| app/routers/analytics.py | **Router central (~140KB)**: endpoints Journey/Network/Tracking (`GET`+`POST`, `response_mode` progresivo). Los 3 rate models de imputación cross-study (`_build_consideration_rate_model`/`_build_satisfaction_rate_model`/`_build_csat_gap_model`) entrenan vía RPC SQL (`bbs_rate_model_training_stats`) sobre `journey_metrics`, con fallback automático al escaneo de parquet (`BBS_RATE_MODEL_SOURCE`). |
| app/routers/network.py | Endpoint `/network` — construye el grafo de demanda (touchpoints ↔ marcas/benchmark). |
| app/routers/filters.py | Opciones de filtros (estudios, taxonomía, demografía, fecha) para la Scope Bar. |
| app/routers/demographics.py | Endpoints y config de dimensiones demográficas. |
| app/routers/taxonomy.py | Endpoints de taxonomía de mercado (sector/subsector/categoría). |
| app/routers/marts.py | Construcción/lectura de marts curados. |
| app/routers/studies.py | CRUD/listado de estudios y su estado en el pipeline. |
| app/routers/study_config.py | Configuración por estudio (columnas base: respondent/weight). |
| app/routers/ingest.py | Ingesta de archivos `.sav` desde `data/landing`. |
| app/routers/mapping.py | Mapeo de preguntas/columnas del estudio a esquema canónico. |
| app/routers/pipeline.py | Orquesta el pipeline de ingesta→mapeo→curado. Incluye `_apply_consideration_from_purchase_override` — corrección per-estudio (gateada por `methodology_overrides.consideration_from_purchase`) para estudios donde consideración y compra se preguntaron simultáneamente. |
| app/routers/questions.py | Endpoints de catálogo de preguntas. |
| app/routers/question_map.py | CRUD del mapa de preguntas por estudio. |
| app/routers/rules.py | Reglas de clasificación/taxonomía (motor de reglas). |
| app/routers/health.py | Healthcheck. |
| app/data/ingest_from_landing.py | Convierte `.sav` de `data/landing` a parquet crudo/curado. |
| app/data/sav_reader.py | Lectura de archivos SPSS `.sav` (`pyreadstat`). |
| app/data/rule_engine.py | Motor de reglas de clasificación/inferencia de mapeo. |
| app/data/market_lens.py | Resolución de clasificación de mercado (sector/subsector/categoría) por estudio. |
| app/data/demographics.py | Config y utilidades de dimensiones demográficas. |
| app/data/study_config.py | Detección de columnas base (`detect_base_columns`) por estudio; también `load_methodology_overrides`/`save_methodology_overrides` (blob separado, sobrevive al guardado de columnas base). |
| app/data/warehouse.py | Helpers de acceso a datos (parquet/csv/json) respaldados por Supabase Storage — reemplazó el acceso a disco local + DuckDB en `data/warehouse.duckdb`. |
| app/data/standardize.py | Placeholder — estandarización de esquemas cliente-específicos (no implementado aún). |
| app/storage/blob.py | `SupabaseStorage` — cliente REST (vía `httpx`) para el bucket de Storage donde vive todo el pipeline (ingesta, raw/curated, mapping, reglas, config). Cada lectura anexa un query param de cache-busting — Supabase Storage está detrás de un CDN Cloudflare que puede servir un GET con días de antigüedad tras un write exitoso al mismo key. |
| app/storage/question_map.py | Persistencia del mapa de preguntas. |
| app/storage/postgres_rpc.py | `SupabasePostgresRpc` — cliente REST (`httpx`, sin SDK) contra PostgREST RPC de Supabase; primer acceso directo de `services/api` a Postgres (antes solo hablaba con Storage). |
| tests/test_study_config.py | Tests unitarios (`unittest`) de `detect_base_columns`. |
| tests/test_pipeline_overrides.py | Tests de `_apply_consideration_from_purchase_override` (fuerza consideración=1 cuando hay compra=1 sin considerar). |

## [scripts/](scripts/) — utilidades locales
| Archivo | Descripción |
|---|---|
| run_api.ps1 | Levanta el venv + uvicorn del API (rutas hardcodeadas al disco del autor — ajustar `$ApiRoot`). |
| run_web.ps1 | `npm install` + `npm run dev` (rutas hardcodeadas al disco del autor — ajustar `$WebRoot`). |
| export_supabase_seed.py | Bootstrap: siembra tablas iniciales de Supabase desde outputs locales del API. |
| export_storage_seed.py | Sube `data/landing` + `data/warehouse` (local, gitignored) al bucket de Supabase Storage — correr una sola vez antes de apuntar Render al proyecto real. |

## [supabase/sql/](supabase/sql/) — contrato y migraciones SQL
Migraciones numeradas, aplicadas en orden en el SQL Editor de Supabase. `001_bbs_rpc_contract.sql`
(el scaffold del contrato RPC mencionado en el README) está en `.gitignore` como borrador local —
no está trackeado en git.

| Archivo | Descripción |
|---|---|
| 002_bbs_seed_tables.sql | Tablas prerequisito para el script de seed. |
| 003_data_phase1_ops.sql | Tablas del módulo Data Ops: `uploaded_files`, `ingestion_jobs`, `ingestion_job_logs`, `data_versions`. |
| 004_admin_rbac_scopes.sql | Tablas de RBAC: `user_roles`, `role_permissions`, `user_permissions`, `user_access_scopes`. |
| 005_market_lens_dual_taxonomy.sql | Soporte de doble taxonomía (legado + market lens) en tablas de mercado. |
| 006–009_*.sql | Fixes iterativos de `bbs_tracking_series` respecto a columnas de mercado. |
| 010–012_market_lens_*.sql | Ajustes de clasificación de mercado (Home Improvement, Fashion/Discount Retail, Mass Retail). |
| 018_fix_bbs_filters_options_demographics.sql | Fix del RPC de opciones demográficas. |
| 019_expand_user_access_scopes_market_types.sql | Expande tipos de scope de acceso de usuario. |
| 020_agent_chat_module.sql | Tablas del módulo Agente (conversaciones/mensajes). |
| 021–023_*_imputation_columns.sql | Columnas de imputación (consideration/satisfaction/CSAT) para manejo de missingness. |
| 013_market_lens_beverages_beer.sql | Agrega Cervezas/Bebidas alcohólicas/no alcohólicas a los fallbacks `bbs_market_subsector`/`bbs_market_category` (antes sin ninguna rama para bebidas). |
| 024_rate_model_training_stats_rpc.sql | `bbs_rate_model_training_stats` — entrena los 3 rate models de imputación (consideration/satisfaction/csat) vía SQL sobre `journey_metrics`, reemplazando el escaneo de parquet de todo el corpus en cada Push. |
| 025_fix_consideration_source_mislabel_backfill.sql | Backfill de una fila con `brand_consideration_source` mal etiquetado (bug corregido en `analytics.py`). |
| 026_capture_live_rpc_definitions.sql | Captura de gobernanza (no-op) del DDL real de `bbs_journey_table_multi`/`bbs_touchpoints_table_multi`/`bbs_tracking_series`/`bbs_network`, que solo vivían en el editor SQL de Supabase. |
| 027_market_lens_footwear.sql | Agrega Calzado/Calzado deportivo a los fallbacks `bbs_market_subsector`/`bbs_market_category` (mismo patrón que 013 para bebidas). |

## [data/](data/) — warehouse local (no trackeado en git)
Ver detalle en [CLAUDE.md](CLAUDE.md) § "Datos locales". Contiene `landing/` (`.sav` fuente),
`warehouse/raw`, `warehouse/curated` (marts por `study_id=...`), `warehouse/demographics`,
`warehouse/study_config`, `warehouse/taxonomy`, `warehouse/mapping`, y `warehouse.duckdb`.
