WITH

{{ dimensional_dbt.source_builder('_stripe_source','chiquita_product_id::NUMBER', 'stripe') }}
,{{ dimensional_dbt.source_builder(['snapshots', 'bluth_wms_inventory'], "REPLACE(vendor_id,'chi-','')::NUMBER", 'warehouse', "source") }}
,{{ dimensional_dbt.source_builder('DIMENSIONAL_DBT_SNAPSHOTS.CHIQUITA_INVOICES','id::NUMBER', 'vendor', "raw") }}

{% call dimensional_dbt.column_selection(['stripe','warehouse','vendor'],6 ,["","warehouse_d.vendor_id = 'chi-500'",""]) %}
    SELECT 
        stripe_d.name AS product_name
        ,stripe_d.retail_price AS retail_price
        ,vendor_d.wholesale_cost AS wholesale_cost
        ,stripe_d.description AS description
        ,warehouse_d.vendor_id AS vendor_id
        ,IFNULL(warehouse_d.storage_type,'Storage Unkown') AS required_type_of_storage_in_warehouse
{% endcall %}