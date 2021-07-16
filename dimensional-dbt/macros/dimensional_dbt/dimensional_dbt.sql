{%- macro generate_source_ctes(source_ctes, unique_key, precision="hour") -%}
    {#/* creates the dim-ready source sets for merging dimensions.
        Args:
            source_ctes: array of prepaired dbt_snapshots
            unique_key: the identifier or expression 
                for the unique id shared by _all_ source ctes.
            precision: if set, will alter the truncation window of the snapshots.
                default is hourly precision.
        Notes:
            - This macro MUST be called after a `WITH` block (it productes CTEs)
            - This macro is required BEFORE your final select statment!
        Returns:
            a group of prefixed CTEs ready for the final select statement.        
    */#}
    {% for source_cte in source_ctes %}
        {% ',' if not loop.first %}
        {{source_cte}}_truncated AS (
            {{ dimensional_dbt._truncate_snapshots(source_cte, unique_key, precision) }}
        )
        ,{{source_cte}}_spine AS (
            {{ dimensional_dbt._generate_spine( source_cte ~ '_truncated', unique_key) }}
        )
    {% endfor %}

        ,dim_valid_window AS (
            WITH 
            complete_spine AS (
                {{ dimensional_dbt._merge_spines(source_ctes) }}
            )
            ,duration_window AS (
                {{ dimensional_dbt._create_duration_windows_from_spine('complete_spine') }}
            )
            SELECT
                *
            FROM 
                duration_window
        )        

{%- endmacro -%}

{%- macro from_clause(source_ctes) -%}
    dim_valid_window 
    {%- for source_cte in source_ctes -%}
        INNER JOIN
        {{source_cte}}_truncated AS {{source_cte}}_d
        ON dim_valid_window.unique_key = {{source_cte}}_d.unique_key
        AND {{source_cte}}_d.dim_valid_to > dim_valid_window.dim_valid_from
        AND {{source_cte}}_d.dim_valid_from < dim_valid_window.dim_valid_to
    {%- endfor -%}

{%- endmacro -%}

{%- macro dim_columns(unique_key) -%}
    ROW_NUMBER() OVER( ORDER BY dbt_updated_at) AS {{target.name}}_key
    ,dim_valid_window.unique_key AS {{ target.name }}_id
    ,dim_valid_window.dim_valid_from
    ,dim_valid_window.dim_valid_to
    ,CASE dim_valid_window.dim_valid_to
        WHEN '9999-12-31'::TIMESTAMP_NTZ THEN TRUE
        ELSE FALSE
    END AS dim_is_current_record
{%- endmacro -%}