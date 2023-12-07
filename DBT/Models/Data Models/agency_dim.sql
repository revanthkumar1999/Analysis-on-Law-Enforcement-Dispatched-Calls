{{ config(materialized="view") }}


select row_number() over (order by agency) as agency_id, *
from
    (

        select distinct agency, disposition, onview_flag, sensitive_call
        from raw_curation.le_curr_curated
    )
