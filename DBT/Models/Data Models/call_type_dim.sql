{{ config(materialized="view") }}

select row_number() over (order by call_type_original) as call_type_id, *
from
    (
        select distinct

            call_type_original,
            call_type_original_desc,
            call_type_final,
            call_type_final_desc
        from raw_curation.le_curr_curated
    )
