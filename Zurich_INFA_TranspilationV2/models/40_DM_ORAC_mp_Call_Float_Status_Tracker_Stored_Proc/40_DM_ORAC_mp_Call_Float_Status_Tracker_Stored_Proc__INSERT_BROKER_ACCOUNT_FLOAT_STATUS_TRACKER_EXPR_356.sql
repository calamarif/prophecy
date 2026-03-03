{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH INSERT_BROKER_ACCOUNT_FLOAT_STATUS_TRACKER AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_Call_Float_Status_Tracker_Stored_Proc', 
      'INSERT_BROKER_ACCOUNT_FLOAT_STATUS_TRACKER'
    )
  }}

),

INSERT_BROKER_ACCOUNT_FLOAT_STATUS_TRACKER_EXPR_356 AS (

  SELECT RETURN_VALUE AS DUMMY
  
  FROM INSERT_BROKER_ACCOUNT_FLOAT_STATUS_TRACKER AS in0

)

SELECT *

FROM INSERT_BROKER_ACCOUNT_FLOAT_STATUS_TRACKER_EXPR_356
