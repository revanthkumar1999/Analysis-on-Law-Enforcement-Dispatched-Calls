{{ config(materialized="view") }}


select row_number() over (order by priority_orginal) as priority_id, *
from
    (select distinct priority_orginal, priority_final from raw_curation.le_curr_curated)
