{{
    config(
        materialized='incremental',
        unique_key='cad_number'
    )
}}





select 
cad_number
,received_datetime
,close_datetime
,call_type_final
,priority_final
,agency
,intersection_point
,police_district
from raw_raw.le_arc