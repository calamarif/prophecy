{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH execute_data_refresh_event AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('40_DM_ORAC_mp_data_refresh_master', 'execute_data_refresh_event') }}

),

DUMMY_TARGET_EXP AS (

  SELECT 
    RETURN_VALUE AS NEWFIELD,
    CAST(NULL AS string) AS NEWFIELD1,
    CAST(NULL AS string) AS NEWFIELD2,
    CAST(NULL AS string) AS NEWFIELD3,
    CAST(NULL AS string) AS NEWFIELD4
  
  FROM execute_data_refresh_event AS in0

)

SELECT *

FROM DUMMY_TARGET_EXP
