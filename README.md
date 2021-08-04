# Dimensional DBT

_Finally, a Kimball Dimensional model toolkit for DBT that **actually works**._

## What is this and what does it do? 
DBT Snapshots are great, and they fit nicely into the [Functional Data Engineering](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) paradigm. 

Kimball Dimensions are great, and they make it easy and fast to query complex timeseries data. 

Too bad these architectures do not play well together... 

**UNTIL NOW**. 

[dimensional_dbt](https://github.com/palmetto/dimensional_dbt) gives you the best of both worlds; use [DBT Snapshots](https://docs.getdbt.com/docs/building-a-dbt-project/snapshots) to make your source data immutable, then use `dimensional_dbt` to create true [slowly changing dimensions](https://www.kimballgroup.com/2008/08/slowly-changing-dimensions/) from those snapshots. 


### What dimensional_dbt gives you

* Combine multiple snapshots into a single slowly changing dimension
* De-duplicate snapshots below the precision threshold (default is hourly) ~ _TODO_: this is not adjustable right now, need to fix that.
* Kimball-correct timespans (beginning-of-time to end-of-time)
* Numeric dimensional keys


## Installing dimensional_dbt

Install the package via [dbthub](https://hub.getdbt.com/) by adding to your packages.yml:

```
packages:
  - package: palmetto/dimensional_dbt
    version: 0.0.1
```

Or install directly from git:

```
packages:
  - git: "https://github.com/palmetto/dimensional_dbt.git"
    revision: 0.0.1 
```

## Use Patterns

`dimensional_dbt` stitches dbt snapshot tables together, so first things first make sure you are creating snapshots with `dbt snapshot`. 

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
* `<your_model_name>_sk` the numeric identifier assigned by the model name. Each record has a unique key representing the record at that instance in time.
* `<your_model_name>_id` the unique identifier of the object, common to keys that represent different states of the same object.


## Transformation Strategies

There are two strategies for where to do your transformations:
1. upstream of `dimensional_dbt` in an ephemeral model
2. downstream of `dimensional_dbt` after the merge

Both use the same structure above. 
Generally it is recommended to do your transforms upstream of the merge where possible; this makes transform code easy to isolate and debug.
The dbt [Ephemeral](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations#ephemeral) pattern is great for this; 
You can create ephemeral models for each of your dimensional sources, then stitch the transformed data together.

## NULL value handling with coalesce

Kimball methodology for dimensions [recommends against allowing NULL values in dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/null-dimension-attribute/#:~:text=Null%2Dvalued%20dimension%20attributes%20result,place%20of%20the%20null%20value.), and with good reasons:

- `NULL` values do not always play well with BI Tools and join patterns, 
   because missing data is not the same as a symbol like "Not Available"
- Meaningful symbols such as "Not Available" make rows readable without header context
- If values are always explicit, then `NULL`s can be used as a warning sign for bad business logic

To implement a non-null dimensional pattern, `COALESCE` is your friend. In the `column_selection` block 
you can use coalesce to determine the order of precedence for sources, and a final default value.
In the example above, if we want to define a customer's contact preference first by the CRM value, then the ERP value, or 
indicate no preference has been given:

```
// In column_selection macro
...
, COALESCE(crm_d.contact_preference, erp_d.contact_preference, "No Contact Preference Given") AS contact_preference

```

### Adding the Dimension Key to a Fact
Dimensions are only really useful once they start adding value to facts. To do that easily, `dimensional_dbt` ships with the
**dimensional_dbt.dim_lookup** macro. 

```
// fact sale

SELECT
  invoice.date AS sale_date
  ,invoice.quantity AS quantity
  ,invoice.total_price AS total_price
  ,dim_user.dim_user_sk AS dim_user_sk
...

FROM
  source_database.customer_invoices invoice
  {{ dimensional_dbt.dim_lookup('dim_user', 'invoice.erp_user_id', 'invoice.date') }}

```

`dim_lookup` takes 3 required arguments and 2 optional:
* dimensional_model: the name of the dim you want to include the key for
* identifier: the column or sql statement for the identifier that matches the dim_id
* occurance_at: the timestamp or date at which the fact occurred (so dimensional_dbt can find the correct key)
* alias (optional): an alias for the joined dim. useful when you need the same dim more than once
* current (optional): when True, dimensional_dbt will ignore the occurance_at value and join 
  the most current record for the id (ie "current state")

## Troubleshooting

### Common Errors:

- `sequence item <int>: expected str instance, Undefined found`
  This is likely because you haven't actually installed the `dimensional_dbt` package. 
  Try running `dbt clean && dbt deps`.