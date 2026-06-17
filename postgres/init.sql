-- ─────────────────────────────────────────────────────────────────
-- Energy Data Pipeline — Schéma PostgreSQL
-- ─────────────────────────────────────────────────────────────────

-- Table principale des KPIs par catégorie et par exécution
CREATE TABLE IF NOT EXISTS public.energy_kpi_by_category (
    id                  SERIAL PRIMARY KEY,
    date_execution      DATE NOT NULL,
    activity_category   VARCHAR(255) NOT NULL,
    nb_declarations     INTEGER,
    total_surface_m2    NUMERIC(15, 2),
    total_consumption   NUMERIC(15, 2),   -- kWh
    avg_consumption     NUMERIC(15, 2),   -- kWh moyen par déclaration
    avg_consumption_m2  NUMERIC(15, 4),   -- kWh/m² (intensité énergétique)
    CONSTRAINT energy_kpi_category_unique UNIQUE (date_execution, activity_category)
);

-- Table du résumé quotidien global
CREATE TABLE IF NOT EXISTS public.energy_kpi_daily_summary (
    id                      SERIAL PRIMARY KEY,
    date_execution          DATE NOT NULL UNIQUE,
    total_declarations      INTEGER,
    total_consumption_kwh   NUMERIC(15, 2),
    avg_consumption_kwh     NUMERIC(15, 2),
    top_category            VARCHAR(255),   -- catégorie la plus énergivore
    rows_processed          INTEGER,
    quality_ratio           NUMERIC(5, 4),  -- % lignes valides (data quality)
    pipeline_duration_s     INTEGER
);

-- Vue : top 5 des secteurs les plus énergivores (dernière exécution)
CREATE OR REPLACE VIEW public.v_top5_energy_consumers AS
SELECT
    activity_category,
    avg_consumption_m2,
    nb_declarations,
    total_consumption
FROM public.energy_kpi_by_category
WHERE date_execution = (SELECT MAX(date_execution) FROM public.energy_kpi_by_category)
ORDER BY avg_consumption_m2 DESC
LIMIT 5;
