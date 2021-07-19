{%- macro column_selection(source_ctes, column_count) -%}
    {#/* The dimensional merge entrypoint where final select columns are defined.
        Note: this is intended to be used as a callback for a `call` block with a SELECT partial.
        Args:
            source_ctes: a list of CTE names either manually created or generated via `source_builder`. 
                Each CTE will be merged into the final available sources for the select statement.
            column_count: The number of columns captured in the select.
        Returns:
            A completed complex partial that wraps the calling SELECT with a CTE and complex FROM statement. 
            See the README for more details.
    */#}
    {{ dimensional_dbt.before_select(source_ctes) }}
    {{ caller() }}
    {{ dimensional_dbt.after_select(source_ctes, column_count) }}
{%- endmacro -%}


{%- macro source_builder(source_value, unique_identifier, alias=none, source_type="ref") -%}
    {#/* Utility for creating source CTEs that are compatible with the `column_selection` macro.
        Args:
            source_value: the name of the ref, raw relation path, or a list of parts for a source call.
            unique_identifier: the column name or sql statement to be used to access the unifying identifier
            alias: if provided, a short name for the CTE
            source_type: how should source_value be processed? default is `{{ref(source_value)}}` 
        RETURNS:
            a CTE compatable with `column_selection`
    */#}
    {%- set cte_name = alias if alias else source_ref.name -%}
    {{cte_name}} AS (
        SELECT
            *
            ,{{unique_identifier}} AS dimensional_dbt_unique_identifier
        FROM 
        {% if source_type == "ref" %}
            {{ ref(source_value) }}
        {% elif source_type == "source" %}
            {{ source(source_value[0], source_value[1]) }}
        {% else %}
            {{ source_value }}
        {% endif %}
    )
{%- endmacro -%}