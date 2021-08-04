SELECT
    dim_product.sk AS dim_product_key
    ,sale.quantity AS quantity_sold
    ,sale.sale_tendered::DATE as sold_on_date
FROM
    {{ source('snapshots', 'bluth_stripe_order_events') }} AS sale
{{ dimensional_dbt.dim_lookup('dim_product_fully_abstracted', 'sale.vendor_product_id', 'sale.sale_tendered', 'dim_product')}}