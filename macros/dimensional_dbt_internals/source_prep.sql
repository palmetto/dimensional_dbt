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
        {% if not loop.first %}, {% endif %}
        {{source_cte}}_truncated AS (
            {{ dimensional_dbt._truncate_snapshots(source_cte, unique_key, precision) }}
        )
        ,{{source_cte}}_spine AS (
            {{ dimensional_dbt._generate_spine( source_cte ~ '_truncated', unique_key) }}
        )
    {% endfor %}


    {#/* jinja hack to get us a list of truncated CTEs. */#}
    {% set truncated_ctes = [] %}
    {% for cte in source_ctes %}
        {% set trunacted_ctes = truncated_ctes.append(cte ~ '_truncated') %}
    {% endfor %}


        ,dim_valid_window AS (
            WITH

            {#/* similar to the hack above, this time we piggyback the loop */#}
            {% set spine_ctes = [] %} 
            {% for cte in source_ctes %}
                {{cte}}_spine AS (
                    {{ dimensional_dbt._generate_spine(cte ~ '_truncated', 'dimensional_dbt_unique_key') }}
                )
                {% if not loop.last %},{% endif %}
                {% set spine_ctes = spine_ctes.append(cte ~ '_spine') %}
            {% endfor %}
            
            ,complete_spine AS (
                {{ dimensional_dbt._merge_spines(spine_ctes) }}
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

{%- macro from_clause(source_ctes, column_count, where_clauses) -%}
    {#/* generates the final predicate.
        Args:
            source_ctes: the array of ctes to build the clause for.
            column_count: int, the number of total colums (not including dimensional_dbt freebies) you ended up using.
        Returns:
            The `FROM` partial. expects a `FROM ` to proceed it.
    */#}

    dim_valid_window 
    {% for source_cte in source_ctes %}
        LEFT JOIN
        {{source_cte}}_truncated AS {{source_cte}}_d
        ON dim_valid_window.dimensional_dbt_unique_key = {{source_cte}}_d.dimensional_dbt_unique_key
        AND {{source_cte}}_d.dimensional_dbt_valid_to > dim_valid_window.dim_valid_from
        AND {{source_cte}}_d.dimensional_dbt_valid_from < dim_valid_window.dim_valid_to
    {% endfor %}
    {% for where_clause in where_clauses %}
        {% if where_clauses and loop.first %}
            WHERE
        {% endif %}

        {% if where_clause|length %}
            {{where_clause}}
        {% endif %}

        {% if not loop.last and where_clauses[loop.index + 1]|length %}
            AND
        {% endif %}

    {% endfor %}
    GROUP BY {% for _ in range(column_count + 1) %}{{loop.index}}{% if not loop.last %},{% endif %}{% endfor %}

{%- endmacro -%}

{%- macro dim_columns() -%}
    dim_valid_window.dimensional_dbt_unique_key AS {{ this.name }}_id
    ,MAX(CASE dim_valid_window.dim_valid_to
        WHEN '9999-12-31'::TIMESTAMPNTZ THEN TRUE
        ELSE FALSE
    END) AS dim_is_current_record
    ,MIN(dim_valid_window.dim_valid_from) AS dim_valid_from
    ,MAX(dim_valid_window.dim_valid_to) AS dim_valid_to
{%- endmacro -%}

{%- macro generate_dim_key() -%}
    {#/* Actually creates the dim key */#}
    ROW_NUMBER() OVER(ORDER BY 1) + 5000 AS sk
{%- endmacro -%}

{%- macro dim_key() -%}
    sk
{%- endmacro -%}