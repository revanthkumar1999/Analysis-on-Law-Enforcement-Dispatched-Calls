{{ config(materialized="view") }}


select row_number() over (order by intersection_name) as location_id, *
from
    (
        select distinct
            intersection_name,
            intersection_id,
            intersection_point,
            supervisor_district,
            analysis_neighborhood,
            police_district
        from raw_curation.le_curr_curated
    )
