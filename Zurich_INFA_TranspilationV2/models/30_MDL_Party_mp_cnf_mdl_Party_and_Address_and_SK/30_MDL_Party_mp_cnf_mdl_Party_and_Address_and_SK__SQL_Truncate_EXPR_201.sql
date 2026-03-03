{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH SQL_Truncate AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('30_MDL_Party_mp_cnf_mdl_Party_and_Address_and_SK', 'SQL_Truncate') }}

),

SQL_Truncate_EXPR_201 AS (

  SELECT SQLError AS ERROR_STRING
  
  FROM SQL_Truncate AS in0

)

SELECT *

FROM SQL_Truncate_EXPR_201
