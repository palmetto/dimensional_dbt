WITH

a_spine AS (
    SELECT
        x.value:spine_value::TIMESTAMP_NTZ AS spine_value
        ,x.value:unique_key AS unique_key
    FROM
        TABLE(FLATTEN(input => parse_json(
        '[
            {"spine_value":"2020-11-05 12:00:00", "unique_key": 505050},
            {"spine_value":"2020-11-05 15:00:00", "unique_key": 505050},
            {"spine_value":"0000-01-01 00:00:00", "unique_key": 505050},
            {"spine_value":"9999-12-31 00:00:00", "unique_key": 505050}
         ]'
         ))) x
   
)
,
b_spine AS (
    SELECT
        x.value:spine_value::TIMESTAMP_NTZ AS spine_value
        ,x.value:unique_key AS unique_key
    FROM
        TABLE(FLATTEN(input => parse_json(
        '[
            {"spine_value":"2020-10-15 09:00:00", "unique_key": 505050},
            {"spine_value":"2020-11-03 10:00:00", "unique_key": 560154},
            {"spine_value":"0000-01-01 00:00:00", "unique_key": 560154},
            {"spine_value":"9999-12-31 00:00:00", "unique_key": 505050},
            {"spine_value":"9999-12-31 00:00:00", "unique_key": 560154},
            {"spine_value":"0000-01-01 00:00:00", "unique_key": 505050}
         ]'
         ))) x
   
)
,c_spine AS (
    SELECT
        x.value:spine_value::TIMESTAMP_NTZ AS spine_value
        ,x.value:unique_key AS unique_key
    FROM
        TABLE(FLATTEN(input => parse_json(
        '[
            {"spine_value":"2018-05-11 19:00:00", "unique_key": 505050},
            {"spine_value":"2021-04-13 11:00:00", "unique_key": 560154},
            {"spine_value":"0000-01-01 00:00:00", "unique_key": 560154},
            {"spine_value":"9999-12-31 00:00:00", "unique_key": 505050},
            {"spine_value":"9999-12-31 00:00:00", "unique_key": 560154},
            {"spine_value":"0000-01-01 00:00:00", "unique_key": 505050}
         ]'
         ))) x
   
)

, test_of_spine_merge AS (
        {{ dimensional_dbt._merge_spines(['a_spine','b_spine','c_spine']) }}
)

, expected_505050_spine AS (
    SELECT 
        x.value:spine_value::TIMESTAMP_NTZ AS spine_value
        ,x.value:unique_key AS unique_key
    FROM
        TABLE(FLATTEN(input => parse_json(
        '[
            {"spine_value":"0000-01-01 00:00:00", "unique_key": 505050},
            {"spine_value":"2018-05-11 19:00:00", "unique_key": 505050},
            {"spine_value":"2020-10-15 09:00:00", "unique_key": 505050},
            {"spine_value":"2020-11-05 12:00:00", "unique_key": 505050},
            {"spine_value":"2020-11-05 15:00:00", "unique_key": 505050},
            {"spine_value":"9999-12-31 00:00:00", "unique_key": 505050}
         ]'
         ))) x
) 

,expected_not_found AS (
    SELECT
        *
    FROM
        expected_505050_spine
    MINUS
    SELECT
        *
    FROM
        test_of_spine_merge
    WHERE 
        unique_key = 505050
)
,found_not_expected AS (
    SELECT
        *
    FROM
        test_of_spine_merge
    WHERE
        unique_key = 505050

    MINUS
    SELECT
        *
    FROM
        expected_505050_spine
)

SELECT 
    *
FROM 
    expected_not_found
UNION
SELECT
    *
FROM
    found_not_expected