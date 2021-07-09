WITH

salesforce_leads AS (
	SELECT
		email
		, id
		, company
		, converteddate
		, isconverted
		, leadsource
		, status
		, dbt_scd_id
		, dbt_updated_at
		, dbt_valid_from
		, dbt_valid_to
	FROM 
		{{ source('salesforce_datalake', 'lead')}}
)

,loan_prequalifications AS (
	SELECT
		prequal_results_data
		, _id
		, financier
		, alchemy_contact_id
		, is_active
		, dbt_scd_id
		, dbt_run_timestamp
		, dbt_updated_at
		, dbt_valid_from
		, dbt_valid_to
	FROM
		{{ source('alchemy_datalake', 'loan_prequalification_checks') }}
)

,deduplicate_salesforce_lead_emails AS (
	{# Multiple leads exist with the same email address... So dropping all snapshots except for the first email's snapshots #}
	WITH ordered AS (
		SELECT 
			*
			, DENSE_RANK() OVER (PARTITION BY email ORDER BY id) AS rank
		FROM salesforce_leads
	)
	SELECT *
	FROM ordered
	WHERE rank = 1
)

,deduplicate_loan_prequalification_emails AS (
	WITH email_extracted AS (
		SELECT
			_id
			, {{ uniform("app.value:email") }} AS email
			, financier
			, alchemy_contact_id
			, is_active
			, prequal_results_data
			, dbt_scd_id
			, dbt_run_timestamp
			, dbt_updated_at
			, dbt_valid_from
			, dbt_valid_to
			, DENSE_RANK() OVER (PARTITION BY email ORDER BY _id) AS rank
		FROM
			loan_prequalifications base
			,lateral flatten(input=>base.prequal_results_data, path=>'projects') project
			,lateral flatten(input=>project.value, path=>'applicants') app
	)
	SELECT *
	FROM email_extracted
	WHERE rank = 1
)

,hourly_bucketed_salesforce_leads AS (
	{{ hourly_snapshot_records('deduplicate_salesforce_lead_emails', 'email') }}
)

,hourly_bucketed_loan_prequalifications AS (
	{{ hourly_snapshot_records('deduplicate_loan_prequalification_emails', 'email') }}
)

,salesforce_leads_hour_spines AS (
	{{ source_hour_spine('salesforce_leads', 'email', 'hourly_bucketed_salesforce_leads') }}
)

,loan_prequalification_hour_spines AS (
	{{ source_hour_spine('loan_prequalifications', 'email', 'hourly_bucketed_loan_prequalifications') }}
)

/* - build a final to-from range spine like this https://www.oraylis.de/blog/combining-multiple-tables-with-valid-from-to-date-ranges-into-a-single-dimension#:~:text=next%2C%20i%E2%80%99m%20using%20this%20information%20to%20build%20the%20new%20valid%20from%2Fto%20date%20ranges%20by%20using%20a%20window%20function%20to%20perform%20a%20lookup%20for%20the%20next%20date%3A
   - add the hours_spine vals to each original source as valid_from and valid_to
   - window out to the newest record for any unique_key with > 1 valid from of the same value
   - join away on unique_key = unique_key AND one.valid_to > two.valid_from AND one.valid_from < two.valid_to
   - add is_current, create surrogate key and ID
*/
,merged_spines AS (
	{{ merge_spines('salesforce_leads_hour_spines', 'loan_prequalification_hour_spines', 'email') }}
)

,merged_from_and_to AS (
	{{ convert_spine_to_from_and_to('merged_spines', 'spine_hour', 'email') }}
)

SELECT
	s.email as s_email
	, s.company
	, s.isconverted
	, l.email as l_email
	, l.financier
	, l.alchemy_contact_id
	, m.valid_from
	, m.valid_to
FROM
	hourly_bucketed_salesforce_leads s
INNER JOIN merged_from_and_to m
ON 
	s.email = m.email
	AND s.dbt_valid_to_hour > m.valid_from 
	AND s.dbt_valid_from_hour < m.valid_to
LEFT JOIN hourly_bucketed_loan_prequalifications l
ON 
	l.email = m.email 
	AND l.dbt_valid_to_hour > m.valid_from 
	AND l.dbt_valid_from_hour < m.valid_to