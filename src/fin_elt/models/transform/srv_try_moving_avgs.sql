{% set table_exists = engine.execute("select exists (select from pg_tables where tablename = '" + target_table + "')").first()[0] %}

{% if table_exists %}
    {% set max_date = engine.execute("select max(date) from " + target_table).first()[0] %}
{% endif %}

{% if not table_exists %}
CREATE TABLE {{ target_table }} AS
{% endif %}

SELECT date,
       treasury_3mth_yield,
       treasury_2yr_yield,
       treasury_5yr_yield,
       treasury_7yr_yield,
       treasury_10yr_yield,
       ROUND(AVG(treasury_3mth_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS try_3mth_30d_avg,
       ROUND(AVG(treasury_3mth_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             2) AS try_3mth_90d_avg,
       ROUND(AVG(treasury_3mth_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW),
             2) AS try_3mth_180d_avg,
       ROUND(AVG(treasury_2yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS try_2yr_30d_avg,
       ROUND(AVG(treasury_2yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             2) AS try_2yr_90d_avg,
       ROUND(AVG(treasury_2yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW),
             2) AS try_2yr_180d_avg,
       ROUND(AVG(treasury_5yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS try_5yr_30d_avg,
       ROUND(AVG(treasury_5yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             2) AS try_5yr_90d_avg,
       ROUND(AVG(treasury_5yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW),
             2) AS try_5yr_180d_avg,
       ROUND(AVG(treasury_7yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS try_7yr_30d_avg,
       ROUND(AVG(treasury_7yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             2) AS try_7yr_90d_avg,
       ROUND(AVG(treasury_7yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW),
             2) AS try_7yr_180d_avg,
       ROUND(AVG(treasury_10yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS try_10yr_30d_avg,
       ROUND(AVG(treasury_10yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW),
             2) AS try_10yr_90d_avg,
       ROUND(AVG(treasury_10yr_yield::numeric) OVER (ORDER BY date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW),
             2) AS try_10yr_180d_avg

{% if table_exists %}
INSERT INTO {{ target_table }}
SELECT *
FROM stg_treasury_yields
WHERE date > '{{ max_date }}'
{% else %}
FROM stg_treasury_yields
{% endif %}