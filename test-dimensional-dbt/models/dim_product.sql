WITH

stripe AS (
    SELECT
        *
        ,chiquita_product_id::NUMBER AS unique_key
    FROM
        {{ ref('_stripe_source') }}

)
, warehouse AS (
    SELECT
        *
        ,REPLACE(vendor_id,'chi-','')::NUMBER AS unique_key
    FROM
        {{ ref('_warehouse_source') }}
)
, vendor AS (
    SELECT
        *
        ,id::NUMBER AS unique_key
    FROM
        {{ ref('_vendor_source') }}
)
{%- set source_ctes=['stripe','warehouse','vendor'] -%}

, {{ dimensional_dbt.generate_source_ctes(source_ctes, 'unique_key') }}

,column_selection AS (
    SELECT 
        stripe_d.name AS product_name
        ,stripe_d.retail_price AS retail_price
        ,vendor_d.wholesale_cost AS wholesale_cost
        ,IFNULL(warehouse_d.storage_type,'Storage Unkown') AS required_type_of_storage_in_warehouse
        ,{{ dimensional_dbt.dim_columns() }}
    FROM
        {{ dimensional_dbt.from_clause(source_ctes, 4) }}
)
SELECT
    column_selection.*
    ,{{ dimensional_dbt.generate_dim_key() }}
FROM
    column_selection
ORDER BY
    dim_valid_from, 
    {{ dimensional_dbt.dim_key() }}