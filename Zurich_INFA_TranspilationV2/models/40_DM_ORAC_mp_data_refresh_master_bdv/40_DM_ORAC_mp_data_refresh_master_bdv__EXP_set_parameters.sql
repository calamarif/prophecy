{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH DUMMY_SOURCE AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('40_DM_ORAC_mp_data_refresh_master_bdv', 'DUMMY_SOURCE') }}

),

EXP_set_parameters AS (

  SELECT 
    DUMMY AS DUMMY,
    {{ var('CTL_BATCH_ID') }} AS CTL_BATCH_ID,
    {{ var('PMWorkflowRunId') }} AS CTL_JOB_ID,
    (TO_TIMESTAMP({{ var('CTL_LOAD_DM') }}, 'yyyy-MM-dd HH:mm:ss')) AS CTL_LOAD_DM
  
  FROM DUMMY_SOURCE AS in0

)

SELECT *

FROM EXP_set_parameters
