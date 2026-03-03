{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH DUMMY_SOURCE AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('40_DM_ORAC_mp_data_refresh_master_bdv_Claim_Tran', 'DUMMY_SOURCE') }}

),

Exp_Set_Parameters AS (

  SELECT 
    DUMMY AS DUMMY,
    {{ var('CTL_BATCH_ID') }} AS CTL_BATCH_ID,
    {{ var('PMWorkflowRunId') }} AS CTL_JOB_ID,
    (TO_TIMESTAMP({{ var('CTL_LOAD_DM') }}, 'yyyy-MM-dd HH:mm:ss')) AS CTL_LOAD_DM,
    {{ var('MONTHS_PRIOR_COUNT') }} AS Months_Prior_Count
  
  FROM DUMMY_SOURCE AS in0

)

SELECT *

FROM Exp_Set_Parameters
