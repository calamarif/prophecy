{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH INSERT_L_CLAIM_PAYEE_TRAN AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_data_refresh_master_bdv_Claim_Payee_Tran', 
      'INSERT_L_CLAIM_PAYEE_TRAN'
    )
  }}

),

DUMMY_TARGET_EXP AS (

  SELECT 
    RETURN_VALUE AS NEWFIELD,
    CAST(NULL AS string) AS NEWFIELD1,
    CAST(NULL AS string) AS NEWFIELD2,
    CAST(NULL AS string) AS NEWFIELD3,
    CAST(NULL AS string) AS NEWFIELD4
  
  FROM INSERT_L_CLAIM_PAYEE_TRAN AS in0

)

SELECT *

FROM DUMMY_TARGET_EXP
