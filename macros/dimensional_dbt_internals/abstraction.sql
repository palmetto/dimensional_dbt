{%- macro before_select(source_ctes) -%}
    , {{ dimensional_dbt.generate_source_ctes(source_ctes, 'dimensional_dbt_unique_identifier', "HOUR") }}

,dimensional_dbt_column_selection AS (

{%- endmacro -%}

{%- macro after_select(source_ctes, column_count, where_clauses, partial) -%}
    {% if partial %}
        ,{{ dimensional_dbt.coalesce_snapshot_cols(source_ctes, 'valid_from') }}::TIMESTAMPNTZ AS dbt_valid_from
        ,{{ dimensional_dbt.coalesce_snapshot_cols(source_ctes, 'valid_to') }}::TIMESTAMPNTZ AS dbt_valid_to
        ,{{ dimensional_dbt.coalesce_snapshot_cols(source_ctes, 'updated_at') }}::TIMESTAMPNTZ AS dbt_updated_at
        ,{{ dimensional_dbt.coalesce_snapshot_cols(source_ctes, 'scd_id') }} AS dbt_scd_id
    {% else %}
        ,{{ dimensional_dbt.dim_columns() }}
    {% endif %}
    FROM
        {#/* +3 is due to the extra columns above needed for a partial */#}
        {% set final_column_count = (column_count + 3) if partial else column_count %}
        {{ dimensional_dbt.from_clause(source_ctes, final_column_count, where_clauses) }}
)
SELECT
    dimensional_dbt_column_selection.*
    {% if not partial %}
    ,{{ dimensional_dbt.generate_dim_key() }}
    {% endif %}
FROM
    dimensional_dbt_column_selection
ORDER BY
    {% if partial %}
        dbt_valid_from
    {% else %}
        dim_valid_from,
        {{ dimensional_dbt.dim_key() }}
    {% endif %} 
{%- endmacro -%}

