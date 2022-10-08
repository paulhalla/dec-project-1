{% set table_exists = engine.execute("select exists (select from pg_tables where tablename = '" + target_table + "')").first()[0] %}

{% if table_exists %}
    {% set max_date = engine.execute("select max(date) from " + target_table).first()[0] %}
{% endif %}

{% if not table_exists %}
CREATE TABLE {{ target_table }} AS
{% endif %}
WITH yields AS
(
    SELECT date,
           -- fill down previous day's yield where missing (marked as NULL)
           CASE
               WHEN treasury_3mth_yield IS NULL THEN LAG(treasury_3mth_yield) OVER (ORDER BY date)
               ELSE treasury_3mth_yield
               END AS treasury_3mth_yield,
           CASE
               WHEN treasury_2yr_yield IS NULL THEN LAG(treasury_2yr_yield) OVER (ORDER BY date)
               ELSE treasury_2yr_yield
               END AS treasury_2yr_yield,
           CASE
               WHEN treasury_5yr_yield IS NULL THEN LAG(treasury_5yr_yield) OVER (ORDER BY date)
               ELSE treasury_5yr_yield
               END AS treasury_5yr_yield,
           CASE
               WHEN treasury_7yr_yield IS NULL THEN LAG(treasury_7yr_yield) OVER (ORDER BY date)
               ELSE treasury_7yr_yield
               END AS treasury_7yr_yield,
           CASE
               WHEN treasury_10yr_yield IS NULL THEN LAG(treasury_10yr_yield) OVER (ORDER BY date)
               ELSE treasury_10yr_yield
               END AS treasury_10yr_yield
    FROM
        (
        SELECT COALESCE(a.date, b.date, c.date, d.date, e.date)::date AS date,
                     -- handle default values used when markets are closed - replace with null
                     -- cast data types
                     CASE
                         WHEN a.value = '.' THEN NULL
                         ELSE a.value::numeric
                         END                                                AS treasury_3mth_yield,
                     CASE
                         WHEN b.value = '.' THEN NULL
                         ELSE b.value::numeric
                         END                                                AS treasury_2yr_yield,
                     CASE
                         WHEN c.value = '.' THEN NULL
                         ELSE c.value::numeric
                         END                                                AS treasury_5yr_yield,
                     CASE
                         WHEN d.value = '.' THEN NULL
                         ELSE d.value::numeric
                         END                                                AS treasury_7yr_yield,
                     CASE
                         WHEN e.value = '.' THEN NULL
                         ELSE e.value::numeric
                         END                                                AS treasury_10yr_yield
              FROM raw_treasury_yield_3month a
                       FULL JOIN raw_treasury_yield_2year b ON a.date = b.date
                       FULL JOIN raw_treasury_yield_5year c ON a.date = c.date
                       FULL JOIN raw_treasury_yield_7year d ON a.date = d.date
                       FULL JOIN raw_treasury_yield_10year e ON a.date = e.date
        ) t
    WHERE date >= '2000-01-01' -- limit dataset as some fields are missing data for older dates
)
{% if table_exists %}
    INSERT INTO {{ target_table }}
    SELECT *
    FROM yields
    WHERE date > '{{ max_date }}'
{% else %}
    SELECT *
    FROM yields
{% endif %}
