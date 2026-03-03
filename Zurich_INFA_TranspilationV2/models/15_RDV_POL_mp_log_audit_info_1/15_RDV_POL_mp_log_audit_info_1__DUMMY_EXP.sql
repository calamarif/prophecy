{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH EXPTRANS1 AS (

  SELECT *
  
  FROM {{ ref('15_RDV_POL_mp_log_audit_info_1__EXPTRANS1')}}

),

DUMMY_EXP AS (

  SELECT 
    EXECUTED_QUERY AS NEWFIELD,
    ERROR_LOG AS NEWFIELD1,
    CAST(NULL AS string) AS NEWFIELD2,
    CAST(NULL AS string) AS NEWFIELD3
  
  FROM EXPTRANS1 AS in0

)

SELECT *

FROM DUMMY_EXP
