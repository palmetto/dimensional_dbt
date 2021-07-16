{%- macro before_select(source_ctes) -%}
    , {{ dimensional_dbt.generate_source_ctes(source_ctes, 'dimensional_dbt_unique_identifier') }}

,dimensional_dbt_column_selection AS (

{%- endmacro -%}

{%- macro after_select(source_ctes, column_count) -%}
        ,{{ dimensional_dbt.dim_columns() }}
    FROM
        {{ dimensional_dbt.from_clause(source_ctes, column_count) }}
)
SELECT
    dimensional_dbt_column_selection.*
    ,{{ dimensional_dbt.generate_dim_key() }}
FROM
    dimensional_dbt_column_selection
ORDER BY
    dim_valid_from, 
    {{ dimensional_dbt.dim_key() }}
{%- endmacro -%}

{%- macro column_selection(source_ctes, column_count) -%}
    {{ dimensional_dbt.before_select(source_ctes) }}
    {{ caller() }}
    {{ dimensional_dbt.after_select(source_ctes, column_count) }}
{%- endmacro -%}