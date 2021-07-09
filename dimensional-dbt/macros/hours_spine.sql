{% macro _hours_spine(direction, unique_key, source ) %}
    {# TODO: move the trunc logic to hourly_snapshot_records so this becomes trivially simple #}

    SELECT
	    dbt_valid_{{ direction }}_hour AS spine_hour
		, {{ unique_key }}
    FROM 
        {{ source }}
{% endmacro %}


{% macro hourly_snapshot_records(sourcename, unique_key) %}
    {# converts our source to use hour blocks for snapshots. 
       when > 1 snapshot happens in an hour, take the last one. 
    #}
     /* TAKE ONE SNAPSHOT PER HOUR PER EMAIL (UNIQUE_KEY)
     
        convert the dbt_valid_from /to values _first_, 
        window them down to a single row.

        THEN use these new, cleaner ctes in source_hour_spine! 
        use them _again_ when it's time to join the whole jawn. 

        create version of a table with those: 
        
        x trunc'd hourly things
        x min corrected to start of time
        x max corrected to super far off time
        x deduped on hour
     */
    WITH
    min_hours AS (
        SELECT 
            {{ unique_key }}
            , DATE_TRUNC('hour', MIN(dbt_valid_from)) AS min_hour
        FROM 
            {{ sourcename }}
        GROUP BY 1
    )

    , hour_buckets AS (
        SELECT
            {{ unique_key }}
            , dbt_valid_from
            , dbt_valid_to
            , DATE_TRUNC('HOURS', dbt_valid_from) AS dbt_valid_from_hour
            , DATE_TRUNC('HOURS', dbt_valid_to) AS dbt_valid_to_hour
        FROM {{ sourcename }}
    )

    , ranked_hourly_snapshot_records AS (
        SELECT
            {{ unique_key }}
            , dbt_valid_from_hour
            , dbt_valid_to_hour
            , RANK() OVER (PARTITION BY {{ unique_key }}, dbt_valid_from_hour ORDER BY dbt_valid_from DESC) AS rank
        FROM 
            hour_buckets
    )
    
    SELECT
        s.*
        , CASE WHEN
            r.dbt_valid_from_hour = m.min_hour THEN '0000-01-01'::TIMESTAMP_NTZ
        ELSE 
            r.dbt_valid_from_hour
        END AS dbt_valid_from_hour
        , IFNULL(r.dbt_valid_to_hour, '9999-12-31'::TIMESTAMP_NTZ) AS dbt_valid_to_hour
    FROM {{ sourcename }} s
    INNER JOIN ranked_hourly_snapshot_records r
        ON s.{{ unique_key }} = r.{{ unique_key }} AND r.rank = 1
    INNER JOIN min_hours m
        ON m.{{ unique_key }} = r.{{ unique_key }}

{% endmacro %}


{% macro source_hour_spine(sourcename, unique_key, source_cte=none) %}
    {% set source = source_cte if source_cte is not none else sourcename %}
        WITH 
        {{ sourcename }}_hours_from AS (
            {{ _hours_spine('from', unique_key, source) }}
        )
        ,{{ sourcename }}_hours_to AS (
            {{ _hours_spine('to', unique_key, source) }}
        )

        SELECT
            DISTINCT spine_hour
            , {{ unique_key }}
        FROM {{ sourcename }}_hours_from

        UNION

        SELECT 
            DISTINCT spine_hour
            , {{ unique_key }}
        FROM {{ sourcename }}_hours_to

{% endmacro %}

{% macro merge_spines(first_spine, second_spine, unique_key) %}

    WITH
    merged AS (
        SELECT *
        FROM {{ first_spine }}

        UNION

        SELECT *
        FROM {{ second_spine }}
    )

    SELECT *
    FROM merged
    ORDER BY spine_hour

{% endmacro %}

{% macro convert_spine_to_from_and_to(spine_source, time_key, unique_key) %}
   
    select 
        {{ unique_key }}
        , {{ time_key }} as valid_from
        , lead({{ time_key }},1) over (partition by {{ unique_key }} order by {{ time_key }}) as valid_to
    from {{ spine_source }}

{% endmacro %}
