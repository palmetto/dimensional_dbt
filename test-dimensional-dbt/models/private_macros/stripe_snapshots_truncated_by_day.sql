WITH 
stripe_source AS (
    SELECT 
        *
    FROM
        {{ ref('_stripe_source') }}
)

,truncated AS (
    {{ dimensional_dbt._truncate_snapshots('stripe_source', 'chiquita_product_id', 'day') }}
)

SELECT
    *
FROM
    truncated
ORDER BY 
    dbt_updated_at
