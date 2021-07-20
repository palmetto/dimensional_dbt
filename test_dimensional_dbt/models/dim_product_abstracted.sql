WITH

stripe AS (
    SELECT
        *
        ,chiquita_product_id::NUMBER AS dimensional_dbt_unique_identifier
    FROM
        {{ ref('_stripe_source') }}

)
, warehouse AS (
    SELECT
        *
        ,REPLACE(vendor_id,'chi-','')::NUMBER AS dimensional_dbt_unique_identifier
    FROM
        {{ ref('_warehouse_source') }}
)
, vendor AS (
    SELECT
        *
        ,id::NUMBER AS dimensional_dbt_unique_identifier
    FROM
        {{ ref('_vendor_source') }}
)
{%- set source_ctes=['stripe','warehouse','vendor'] -%}

{{ dimensional_dbt.before_select(source_ctes) }}
    SELECT 
        stripe_d.name AS product_name
        ,stripe_d.retail_price AS retail_price
        ,vendor_d.wholesale_cost AS wholesale_cost
        ,stripe_d.description AS description
        ,IFNULL(warehouse_d.storage_type,'Storage Unkown') AS required_type_of_storage_in_warehouse

{{ dimensional_dbt.after_select(source_ctes, 5) }}