{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_mt2` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_mt2'
    )
  }}

),

lkp_REF_CLAIM_TRAN_TYPE_mt2 AS (

  {{ prophecy_basics.ToDo('Component not supported in sql: TLookup') }}

)

SELECT *

FROM lkp_REF_CLAIM_TRAN_TYPE_mt2
