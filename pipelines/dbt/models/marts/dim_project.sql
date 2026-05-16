SELECT
    project_id,
    project_name,
    project_type,
    project_type_desc,
    organization_id,
    target_population,
    latitude,
    longitude
FROM {{ source('hmis_gold', 'dim_project') }}
