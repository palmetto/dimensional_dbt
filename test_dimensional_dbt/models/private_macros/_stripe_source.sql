{{ config(materialized="ephemeral")}}
SELECT
    *
FROM    
    {{ source('snapshots', 'bluth_stripe_banana_stand_product') }}