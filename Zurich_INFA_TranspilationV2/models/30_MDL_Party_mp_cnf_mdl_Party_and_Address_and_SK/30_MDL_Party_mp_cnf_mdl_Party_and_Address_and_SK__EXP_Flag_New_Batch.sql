{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH EXP_collect_source AS (

  SELECT *
  
  FROM {{ ref('30_MDL_Party_mp_cnf_mdl_Party_and_Address_and_SK__EXP_collect_source')}}

),

EXP_Flag_New_Batch AS (

  SELECT (((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) % 1000) = 1) AS NEW_BATCH
  
  FROM EXP_collect_source AS in0

)

SELECT *

FROM EXP_Flag_New_Batch
