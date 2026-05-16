{{ config(materialized='incremental', unique_key='service_id',
          partition_by={'field': 'service_date', 'data_type': 'date'}) }}

SELECT
    s.service_id,
    s.client_id_hash,
    s.enrollment_id,
    s.project_id,
    p.project_type,
    s.service_type,
    s.service_date,
    s.qty,
    s.financial_assistance
FROM   {{ source('hmis_gold', 'fact_service_event') }} s
LEFT   JOIN {{ ref('dim_project') }} p USING (project_id)
{% if is_incremental() %}
WHERE  s.service_date >= (SELECT MAX(service_date) FROM {{ this }})
{% endif %}
