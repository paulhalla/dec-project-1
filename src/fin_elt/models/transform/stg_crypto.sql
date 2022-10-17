{% set table_exists = engine.execute("select exists (select from pg_tables where tablename = '" + target_table + "')").first()[0] %}

{% if table_exists %}
    {% set max_date = engine.execute("select max(date) from " + target_table).first()[0] %}
{% endif %}

{% if not table_exists %}
CREATE TABLE {{ target_table }} AS
{% else %}
INSERT INTO {{ target_table }}
{% endif %}
WITH crypto AS
(
    -- Join all symbols
    -- Add 30 day average
    select
        coalesce(b."Date", d."Date", e."Date") as Date,
        b."Symbol" as "Symbol 1",
        b."Mkt Cap" as "Mkt Cap 1",
        ROUND(AVG(b."Mkt Cap") OVER (ORDER BY b."Date"::date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)::NUMERIC,0) as "Prior 30 Day Avg 1",
        e."Symbol" as "Symbol 2",
        e."Mkt Cap" as "Mkt Cap 2",
        ROUND(AVG(e."Mkt Cap") OVER (ORDER BY e."Date"::date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)::NUMERIC,0) as "Prior 30 Day Avg 2",
        d."Symbol" as "Symbol 3",
        d."Mkt Cap" as "Mkt Cap 3",
        ROUND(AVG(d."Mkt Cap") OVER (ORDER BY d."Date"::date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)::NUMERIC,0) as "Prior 30 Day Avg 3"
    from raw_crypto_price_btc b
    full outer join raw_crypto_price_eth e
        ON b."Date" = e."Date"
    full outer join raw_crypto_price_doge d
        ON b."Date" = d."Date"
    order by b."Date" DESC
)
{% if table_exists %}
    SELECT *
    FROM crypto
    WHERE "Date" > '{{ max_date }}'
{% else %}
    SELECT *
    FROM crypto
{% endif %}
