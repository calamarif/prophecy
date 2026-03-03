{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH DUMMY_SOURCE AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('40_DM_ORAC_mp_date_refresh_event_wait', 'DUMMY_SOURCE') }}

),

EXP_set_parameters AS (

  SELECT 
    DUMMY AS DUMMY,
    {{ var('CTL_BATCH_ID') }} AS ctl_batch_id
  
  FROM DUMMY_SOURCE AS in0

)

SELECT *

FROM EXP_set_parameters
