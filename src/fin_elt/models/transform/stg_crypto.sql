{% set table_exists = engine.execute("select exists (select from pg_tables where tablename = '" + target_table + "')").first()[0] %}

{% if table_exists %}
    {% set max_date = engine.execute("select max(date) from " + target_table).first()[0] %}
{% endif %}

{% if not table_exists %}
CREATE TABLE {{ target_table }} AS
{% endif %}
WITH crypto AS
(
    -- Join all symbols
    -- Add 30 day average
    select
        b.date,
        b.symbol as "Symbol 1",
        b.mkt_cap as "Mkt Cap 1",
        ROUND(AVG(b.mkt_cap) OVER (ORDER BY b.date::date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)::NUMERIC,0) as "Prior 30 Day Avg 1",
        e.symbol as "Symbol 2",
        e.mkt_cap as "Mkt Cap 2",
        ROUND(AVG(e.mkt_cap) OVER (ORDER BY e.date::date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)::NUMERIC,0) as "Prior 30 Day Avg 2",
        d.symbol as "Symbol 3",
        d.mkt_cap as "Mkt Cap 3",
        ROUND(AVG(d.mkt_cap) OVER (ORDER BY d.date::date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)::NUMERIC,0) as "Prior 30 Day Avg 3"
    from raw_crypto_price_btc b
    full outer join raw_crypto_price_eth e
        ON b.date = e.date
    full outer join raw_crypto_price_doge d
        ON b.date = d.date
    order by b.date DESC
)
{% if table_exists %}
    INSERT INTO {{ target_table }}
    SELECT *
    FROM crypto
    WHERE date > '{{ max_date }}'
{% else %}
    SELECT *
    FROM crypto
{% endif %}
