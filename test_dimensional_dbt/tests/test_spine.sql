WITH 
stripe_hourly AS (
    SELECT
        *
    FROM
        {{ ref('stripe_snapshots_truncated_by_hour') }}
)
,spine_from_stripe_hour_snapshots AS (
    {{ dimensional_dbt._generate_spine('stripe_hourly', 'chiquita_product_id') }}
)

SELECT 
    *
FROM 
    spine_from_stripe_hour_snapshots
WHERE
    spine_value NOT IN ('0000-01-01 00:00:00.000'::TIMESTAMP_NTZ,
                        '2021-07-12 14:00:00.000'::TIMESTAMP_NTZ,
                        '2021-07-14 10:00:00.000'::TIMESTAMP_NTZ,
                        '9999-12-31 00:00:00.000'::TIMESTAMP_NTZ) 