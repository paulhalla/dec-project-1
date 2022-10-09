{% set table_exists = engine.execute("select exists (select from pg_tables where tablename = '" + target_table + "')").first()[0] %}

{% if table_exists %}
    {% set max_date = engine.execute("select max(date) from " + target_table).first()[0] %}
{% endif %}

{% if not table_exists %}
CREATE TABLE {{ target_table }} AS
{% endif %}
WITH exchange_rates AS

(select a.date,
        a."1. open" as aud_open_rate,
        a."2. high" as aud_high_rate,
        a."3. low" as aud__low_rate,
        a."4. close" as aud_close_rate,
        b."1. open" as eur_open_rate,
        b."2. high" as eur_high_rate,
        b."3. low" as eur__low_rate,
        b."4. close" as eur_close_rate,
        c."1. open" as jpy_open_rate,
        c."2. high" as jpy_high_rate,
        c."3. low" as jpy__low_rate,
        c."4. close" as jpy_close_rate,
        d."1. open" as rub_open_rate,
        d."2. high" as rub_high_rate,
        d."3. low" as rub__low_rate,
        d."4. close" as rub_close_rate,
        e."1. open" as usd_open_rate,
        e."2. high" as usd_high_rate,
        e."3. low" as usd__low_rate,
        e."4. close" as usd_close_rate
FROM raw_exchange_rate_aud a
                       FULL JOIN raw_exchange_rate_eur b ON a.date = b.date
                       FULL JOIN raw_exchange_rate_jpy c ON a.date = c.date
                       FULL JOIN raw_exchange_rate_rub d ON a.date = d.date
                       FULL JOIN raw_exchange_rate_usd e ON a.date = e.date
                       )

{% if table_exists %}
    INSERT INTO {{ target_table }}
    SELECT *
    FROM exchange_rates
    WHERE date > '{{ max_date }}'
{% else %}
    SELECT *
    FROM exchange_rates
{% endif %}
