{%- macro _truncate_snapshots(source, unique_key, precision='hour') -%}
    {#/* reduces the snapshot granularity to the given precision, and corrects the min/max
         to kimball standard beginning-of-time/end-of-time
        Args:
            source: the cte name or source name of a prepaired DBT snapshot.
            unique_key: the column or expression to uniquely identify same-object rows.
            precision: the level of truncation for each record. 
            One of [these](https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts)
        Returns:
            a complete select with the added columns dimensional_dbt_valid_to
            and dimensional_dbt_valid_from, where earliest and latest values are corrected.
    */#}

    WITH 
        earliest_{{ source }} AS (
            SELECT 
                {{ unique_key }} AS unique_key
                , DATE_TRUNC('{{precision}}', MIN(dbt_updated_at)) AS earliest_dbt_updated_at
            FROM 
                {{ source }}
            GROUP BY 1
        )

        ,truncated_{{ source }} AS (
            SELECT
                {{ unique_key }} AS unique_key
                ,dbt_updated_at
                ,RANK() OVER (PARTITION BY unique_key, DATE_TRUNC('{{precision}}', dbt_updated_at) ORDER BY dbt_updated_at DESC ) AS dimensional_dbt_recency
            FROM
                {{ source }} AS source
        )
    SELECT
        source.*
        ,CASE 
            WHEN DATE_TRUNC('{{precision}}', dbt_valid_from ) = earliest_dbt_updated_at THEN '0000-01-01'::TIMESTAMP_NTZ
            ELSE DATE_TRUNC('{{precision}}', dbt_valid_from )
        END AS dimensional_dbt_valid_from
        ,IFNULL(DATE_TRUNC('{{precision}}', dbt_valid_to ), '9999-12-31'::TIMESTAMP_NTZ) AS dimensional_dbt_valid_to
    FROM
        {{ source }} source
    RIGHT JOIN
        truncated_{{ source }} deduplicated
    ON 
        {{ unique_key }} = deduplicated.unique_key
    AND
        source.dbt_updated_at = deduplicated.dbt_updated_at
    JOIN
    earliest_{{ source }}
    ON 
        1=1
    WHERE 
        dimensional_dbt_recency = 1
{%- endmacro -%}

{%- macro generate_spine(truncated_source, unique_key) -%}

{%- endmacro -%}