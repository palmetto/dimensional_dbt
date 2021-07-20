{{ config(materialized="ephemeral")}}
SELECT
    *
FROM    
    {{ source('snapshots', 'bluth_wms_inventory') }}