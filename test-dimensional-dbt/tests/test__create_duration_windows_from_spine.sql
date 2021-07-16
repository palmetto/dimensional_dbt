WITH
example_spine AS (
    SELECT
        x.value:spine_value::TIMESTAMP_NTZ AS spine_value
        ,x.value:dimensional_dbt_unique_key::VARCHAR AS dimensional_dbt_unique_key
    FROM
        TABLE(FLATTEN(input => parse_json(
        '[
            {"spine_value":"2018-05-11 19:00:00", "dimensional_dbt_unique_key": 505050},
            {"spine_value":"2021-04-13 11:00:00", "dimensional_dbt_unique_key": 560154},
            {"spine_value":"0000-01-01 00:00:00", "dimensional_dbt_unique_key": 560154},
            {"spine_value":"9999-12-31 00:00:00", "dimensional_dbt_unique_key": 505050},
            {"spine_value":"9999-12-31 00:00:00", "dimensional_dbt_unique_key": 560154},
            {"spine_value":"0000-01-01 00:00:00", "dimensional_dbt_unique_key": 505050}
         ]'
         ))) x
)
, under_test AS (
    {{ dimensional_dbt._create_duration_windows_from_spine('example_spine') }}       
)

,expected_values AS (
    SELECT
        x.value:dim_valid_from::TIMESTAMP_NTZ AS dim_valid_from
        ,x.value:dim_valid_to::TIMESTAMP_NTZ AS dim_valid_to
        ,x.value:dimensional_dbt_unique_key::VARCHAR AS dimensional_dbt_unique_key
    FROM
        TABLE(FLATTEN(input => parse_json(
        '[
            {"dim_valid_from":"0000-01-01 00:00:00","dim_valid_to":"2018-05-11 19:00:00","dimensional_dbt_unique_key": 505050},
            {"dim_valid_from":"2018-05-11 19:00:00","dim_valid_to":"9999-12-31 00:00:00","dimensional_dbt_unique_key": 505050},
            {"dim_valid_from":"2021-04-13 11:00:00","dim_valid_to":"9999-12-31 00:00:00", "dimensional_dbt_unique_key": 560154},
            {"dim_valid_from":"0000-01-01 00:00:00","dim_valid_to":"2021-04-13 11:00:00","dimensional_dbt_unique_key": 560154}
         ]'
         ))) x
)

SELECT
    dim_valid_from
    ,dim_valid_to
    ,dimensional_dbt_unique_key
FROM 
    under_test
MINUS
SELECT
    dim_valid_from
    ,dim_valid_to
    ,dimensional_dbt_unique_key
FROM 
    expected_values
