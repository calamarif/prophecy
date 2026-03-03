{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH SQL_Delete_Failed AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '20_STG_ZSX_mp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU', 
      'SQL_Delete_Failed'
    )
  }}

),

SQL_Delete_Failed_EXPR_188 AS (

  SELECT SQLError AS ERROR_STRING
  
  FROM SQL_Delete_Failed AS in0

)

SELECT *

FROM SQL_Delete_Failed_EXPR_188
