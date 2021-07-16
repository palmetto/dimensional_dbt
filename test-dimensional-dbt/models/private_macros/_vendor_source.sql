{{ config(materialized="ephemeral")}}
SELECT
    *
FROM    
    {{ source('snapshots', 'chiquita_invoices') }}