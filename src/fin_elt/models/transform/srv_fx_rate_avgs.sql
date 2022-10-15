{% set table_exists = engine.execute("select exists (select from pg_tables where tablename = '" + target_table + "')").first()[0] %}

{% if table_exists %}
    {% set max_date = engine.execute("select max(date) from " + target_table).first()[0] %}
{% endif %}

{% if not table_exists %}
CREATE TABLE {{ target_table }} AS
{% else %}
INSERT INTO {{ target_table }}
{% endif %}

SELECT date,
      --   a."4. close" as aud_close_rate,
      --   b."4. close" as eur_close_rate,
      --   c."4. close" as jpy_close_rate,
      --   d."4. close" as rub_close_rate,
      --   e."4. close" as gbp_close_rate,
       ROUND(AVG(aud_close_rate::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS aud_3mth_30d_avg,
       ROUND(AVG(eur_close_rate::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS eur_3mth_30d_avg,
       ROUND(AVG(jpy_close_rate::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS jpy_3mth_30d_avg,
       ROUND(AVG(rub_close_rate::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS rub_3mth_30d_avg,
       ROUND(AVG(gbp_close_rate::numeric) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
             2) AS gbp_3mth_30d_avg
FROM stg_exchange_rates
{% if table_exists %}
WHERE date > '{{ max_date }}'
{% endif %}