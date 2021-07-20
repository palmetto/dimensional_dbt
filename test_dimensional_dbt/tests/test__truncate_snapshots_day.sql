WITH 
expected_not_found AS (
    SELECT
        *
    FROM 
        {{ source('tests','expected_stripe_snapshots_truncated_by_day') }}

    MINUS

    SELECT
        *
    FROM 
        {{ ref('stripe_snapshots_truncated_by_day') }}
)

,found_not_expected AS (

    SELECT
        *
    FROM 
        {{ ref('stripe_snapshots_truncated_by_day') }}
    
    MINUS

    SELECT
        *
    FROM 
        {{ source('tests','expected_stripe_snapshots_truncated_by_day') }}
)

SELECT 
    *
FROM
    expected_not_found

UNION ALL

SELECT 
    *
FROM
    found_not_expected
