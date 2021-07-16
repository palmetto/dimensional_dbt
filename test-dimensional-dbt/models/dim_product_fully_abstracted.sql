WITH

{{ dimensional_dbt.source_builder('_stripe_source','chiquita_product_id::NUMBER', 'stripe') }}
,{{ dimensional_dbt.source_builder('_warehouse_source',"REPLACE(vendor_id,'chi-','')::NUMBER", 'warehouse') }}
,{{ dimensional_dbt.source_builder('_vendor_source','id::NUMBER', 'vendor') }}

{% call dimensional_dbt.column_selection(['stripe','warehouse','vendor'], 5) %}
    SELECT 
        stripe_d.name AS product_name
        ,stripe_d.retail_price AS retail_price
        ,vendor_d.wholesale_cost AS wholesale_cost
        ,stripe_d.description AS description
        ,IFNULL(warehouse_d.storage_type,'Storage Unkown') AS required_type_of_storage_in_warehouse
{% endcall %}