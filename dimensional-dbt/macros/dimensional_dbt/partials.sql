{%- macro _truncate_snapshots(source, unique_key, precision='hour') -%}
    {#/* reduces the snapshot granularity to the given precision.
        Args:
            source: the cte name or source name of a prepaired DBT snapshot.
            unique_key: the column or expression to uniquely identify same-object rows.
            precision: the level of truncation for each record. 
            One of [these](https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts)
        Returns:
            a complete select with the added columns dimensional_dbt_valid_to
            and dimensional_dbt_valid_from
    */#}

    WITH 
        truncated_{{ source }} AS (
            SELECT
                {{ unique_key }} AS unique_key
                ,dbt_updated_at
                ,RANK() OVER (PARTITION BY unique_key, DATE_TRUNC('{{precision}}', dbt_updated_at) ORDER BY dbt_updated_at DESC ) AS dimensional_dbt_recency
            FROM
                {{ source }} AS source
        )
    SELECT
        source.*
        ,DATE_TRUNC('{{precision}}', dbt_valid_from ) AS dimensional_dbt_valid_from
        ,DATE_TRUNC('{{precision}}', dbt_valid_to ) AS dimensional_dbt_valid_to
    FROM
        {{ source }} source
    RIGHT JOIN
        truncated_{{ source }} deduplicated
    ON 
        {{ unique_key }} = deduplicated.unique_key
    AND
        source.dbt_updated_at = deduplicated.dbt_updated_at
    WHERE 
        dimensional_dbt_recency = 1
{%- endmacro -%}