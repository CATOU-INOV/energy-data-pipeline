CREATE TABLE IF NOT EXISTS public.energy_data_kpi (
    date_execution DATE,
    activity_category VARCHAR(255),
    average_consumption NUMERIC,
    kpi_score INT,
    CONSTRAINT energy_data_kpi_unique UNIQUE (date_execution, activity_category)
);
