{{ config(materialized="view") }}

with
    call_type_joined as (
        select call_type_id, call_type_original from {{ ref("call_type_dim") }}
    ),

    priority_joined as (
        select priority_id, priority_orginal from {{ ref("priority_dim") }}
    ),

    agency_joined as (select agency_id, agency from {{ ref("agency_dim") }}),

    location_joined as (
        select location_id, intersection_name from {{ ref("location_dim") }}
    )

select
    raw.id,
    raw.cad_number,
    case
        when received_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(received_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as received_datetime,
    case
        when entry_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(entry_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as entry_datetime,
    case
        when dispatch_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(dispatch_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as dispatch_datetime,
    case
        when enroute_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(enroute_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as enroute_datetime,
    case
        when onscene_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(onscene_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as onscene_datetime,
    case
        when close_datetime is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(close_datetime, 'MM/DD/YYYY HH12:MI:SS AM')
    end as close_datetime,
    case
        when call_last_updated_at is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(call_last_updated_at, 'MM/DD/YYYY HH12:MI:SS AM')
    end as call_last_updated_at,
    case
        when data_as_of is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(data_as_of, 'MM/DD/YYYY HH12:MI:SS AM')
    end as data_as_of,
    case
        when data_loaded_at is null
        then to_timestamp('1900-01-01T00:00:00')
        else to_timestamp(data_loaded_at, 'MM/DD/YYYY HH12:MI:SS AM')
    end as data_loaded_at,
    ct.call_type_id as call_type_id_fk,
    p.priority_id as priority_id_fk,
    a.agency_id as agency_id_fk,
    l.location_id as location_id_fk
from raw_curation.le_curr_curated raw
left join call_type_joined ct on raw.call_type_original = ct.call_type_original
left join priority_joined p on raw.priority_orginal = p.priority_orginal
left join agency_joined a on raw.agency = a.agency
left join location_joined l on raw.intersection_name = l.intersection_name
