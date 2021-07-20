{% macro cleanup() %}
    {%- if target.database != "TEST" -%}
          {{ exceptions.raise_compiler_error("Cleanup can only execute in TEST database; currently pointed at " ~ target.database) }}
    {%- endif -%}
    DROP SCHEMA TEST.DIMENSIONAL_DBT;
    DROP SCHEMA TEST.DIMENSIONAL_DBT_SNAPSHOTS;
{% endmacro %}