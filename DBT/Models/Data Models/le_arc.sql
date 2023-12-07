	{{ config(materialized="view") }}


select
    cad_number,
    case
        when received_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(received_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as received_datetime,

    case
        when close_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(close_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as close_datetime,
    call_type_final,
    priority_final,
    agency,
    intersection_point,
    police_district
from raw_curation.le_arc_curated
