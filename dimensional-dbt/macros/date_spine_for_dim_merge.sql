{% macro date_spine_for_dim_merge( cte, unique_key ) %}
	{# creates the union merge records for a snapshotted data source that 
	   is getting merged together. 
		Args:
			cte: the simplified CTE this will build from
			unique_key: the column or expression the spine is using
	
	#}
	SELECT 
		{{ unique_key }}
		, DBT_VALID_FROM as date_point
	FROM
		{{ cte }}
	UNION

	SELECT 
		{{ unique_key }}
		, DBT_VALID_TO as date_point
	FROM
		{{ cte }}
{% endmacro %}