WITH 
expected_not_found AS (
    SELECT
        *
    FROM 
        {{ source('tests','expected_stripe_snapshots_truncated_by_hour') }}

    MINUS

    SELECT
        *
    FROM 
        {{ ref('stripe_snapshots_truncated_by_hour') }}
)

,found_not_expected AS (

    SELECT
        *
    FROM 
        {{ ref('stripe_snapshots_truncated_by_hour') }}
    
    MINUS

    SELECT
        *
    FROM 
        {{ source('tests','expected_stripe_snapshots_truncated_by_hour') }}
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
