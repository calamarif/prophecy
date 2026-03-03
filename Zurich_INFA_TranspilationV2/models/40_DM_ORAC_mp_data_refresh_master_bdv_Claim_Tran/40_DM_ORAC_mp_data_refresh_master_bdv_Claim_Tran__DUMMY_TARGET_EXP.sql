{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH Sp_Insert_S_Claim_Tran AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('40_DM_ORAC_mp_data_refresh_master_bdv_Claim_Tran', 'Sp_Insert_S_Claim_Tran') }}

),

DUMMY_TARGET_EXP AS (

  SELECT 
    RETURN_VALUE AS NEWFIELD,
    CAST(NULL AS string) AS NEWFIELD1,
    CAST(NULL AS string) AS NEWFIELD2,
    CAST(NULL AS string) AS NEWFIELD3,
    CAST(NULL AS string) AS NEWFIELD4
  
  FROM Sp_Insert_S_Claim_Tran AS in0

)

SELECT *

FROM DUMMY_TARGET_EXP
