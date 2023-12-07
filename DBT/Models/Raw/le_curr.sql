{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}


SELECT 
id
,cad_number
,received_datetime
,entry_datetime
,dispatch_datetime
,enroute_datetime
,onscene_datetime
,close_datetime
,call_last_updated_at
,data_as_of
,data_loaded_at
,call_type_original
,call_type_original_desc
,call_type_final
,call_type_final_desc
,priority_orginal
,priority_final
,agency
,disposition
,onview_flag
,sensitive_call
,intersection_name
,intersection_id
,intersection_point
,supervisor_district
,analysis_neighborhood
,police_district
,call_type_original_notes
,call_type_final_notes
,analysis_neighborhoods
FROM 
    raw.le_curr_stage 