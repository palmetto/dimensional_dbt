# Dimensional DBT

_Finally, a Kimball Dimensional model toolkit for DBT that **actually works**._

## What is this and what does it do? 
DBT Snapshots are great, and they fit nicely into the [Functional Data Engineering](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) paradigm. 

Kimball Dimensions are great, and they make it easy and fast to query complex timeseries data. 

Too bad these architectures do not play well together... 

**UNTIL NOW**. 

[dimensional-dbt](https://github.com/palmetto/dimensional-dbt) gives you the best of both worlds; use [DBT Snapshots](https://docs.getdbt.com/docs/building-a-dbt-project/snapshots) to make your source data immutable, then use `dimensional-dbt` to create true [slowly changing dimensions](https://www.kimballgroup.com/2008/08/slowly-changing-dimensions/) from those snapshots. 


### What dimensional-dbt gives you

* Combine multiple snapshots into a single slowly changing dimension
* De-duplicate snapshots below the precision threshold (default is hourly) ~ _TODO_: this is not adjustable right now, need to fix that.
* Kimball-correct timespans (beginning-of-time to end-of-time)
* Numeric dimensional keys


## Installing dimensional-dbt

Install the package via [dbthub](https://hub.getdbt.com/) by adding to your packages.yml:

```
packages:
  - package: palmetto/dimensional_dbt
    version: 0.0.1
```

## Use Patterns

`dimensional-dbt` stitches dbt snapshot tables together, so first things first make sure you are creating snapshots with `dbt snapshot`. 

Once you have snapshots you want to merge, you can do so in a dim model.
For example let's say we have snapshots from our ERP, our CRM and our fraud detection vendor, and we want to merge user data for all 3:

```
/* This is our merge of snapshots.erp.users, snapshots.crm.users, and snapshots.fraud_detection.fraudulent_users */
WITH

/* here we have a [dbt source](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources) of "erp", "users" */ 
{{ dimensional_dbt.source_builder(['erp','users'],'user_id::NUMBER', 'erp', "source") }}

/* For the CRM data we use another model where we do some transforms called PRE_DIM_CRM_USERS.
   We also have to transform the erp_user_id so it matches the other sources */
,{{ dimensional_dbt.source_builder('PRE_DIM_CRM_USERS', "REPLACE(erp_user_id,'user-','')::NUMBER", 'crm') }}

/* we have no ref for the fraud data, so we pass it the raw table name. */
,{{ dimensional_dbt.source_builder('snapshots.fraud_detection.fraudulent_users','erp_user_id::NUMBER', 'fraud', "raw") }}


/* Now call the column_selection to pick which columns you want from each source! */
{% call dimensional_dbt.column_selection(['erp','crm','fraud'], 3) %}
    SELECT 
        COALESCE(erp_d.name, crm_d.name, "No Name Provided") AS name
        ,COALESCE(erp_d.phone, crm_d.home_phone, crm_d.work_phone, "No Phone Provided") AS phone_number
        CASE
            WHEN fraud.id IS NOT NULL THEN "Suspected Fraud" ELSE "Not Suspected Fraud" END AS is_suspected_fraud
{% endcall %}

```
This file will materialize a combined dimension, respecting snapshot changes over time for all three sources! 

In the example above: 

**dimensional_dbt.source_builder** creates a compatible source CTE, and takes 2 required args and 2 optional ones:
* source_value: the "from" for the cte, can be a raw string, model ref or iterable with 2 args for dbt source
* unique_identifier: all the CTEs _must_ share an identifier - this can be any valid sql statement
* alias (optional) the utility name for the final CTE to be queried in the select, will be suffixed with `_d` in the final CTE.
* source_value_type (optional) - one of "ref", "source" or "raw" - tells dimensional_dbt what type of source_value you are passing it. Default is `ref`.

This creates all the source data dimensional_dbt needs to build your stitched dimension from. 
You can then specify how you want to build the stitched dimension via 
**dimensional_dbt.column_selection** with these args:
* source_list: a list of CTE source names created by previously defined `source_builder` calls (or manually as described below)
* column_count: to eliminate duplicate rows created by deltas you are _not_ interested in (no column you specified is capturing them), `column_specification` needs the number of columns to be captured in the select. In the above example we are creating columns `name, phone_number, is_suspected_fraud` so our count would be 3. 

This will result in a table of dimensional snapshots with 1-hour granularity for all 3 sources with the following new columms:
* `dim_valid_from` that reflects greedy (beginning of time) timebands 
* `dim_valid_to` that reflects greedy (end of time) timebands
* `dim_is_current_record` for easy filtering
* `<your_model_name>_key` the numeric identifier assigned by the model name. Each record has a unique key representing the record at that instance in time.
* `<your_model_name>_id` the unique identifier of the object, common to keys that represent different states of the same object.


## Transformation Strategies

Once you have snapshots, there are two strategies for where to do your transformations:
1. upstream of `dimensional-dbt` in an ephemeral model
2. downstream of `dimensional-dbt` after the merge

Both use the same structure, 