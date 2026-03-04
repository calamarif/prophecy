{{
  config({    
    "materialized": "table",
    "alias": "H_REF17",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `15_RDV_POLmp_rdv_H_REF_S_POL_MISS_v_POL_MISS34` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '15_RDV_POL_mp_rdv_H_REF_S_POL_MISS', 
      '15_RDV_POLmp_rdv_H_REF_S_POL_MISS_v_POL_MISS34'
    )
  }}

),

`15_RDV_POLmp_rdv_H_REF_S_POL_MISS_H_REF33` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '15_RDV_POL_mp_rdv_H_REF_S_POL_MISS', 
      '15_RDV_POLmp_rdv_H_REF_S_POL_MISS_H_REF33'
    )
  }}

),

Remove_Duplicates_EXPR_244 AS (

  SELECT 
    REF_HK AS SRC_REF_HK,
    REF_BK AS SRC_REF_BK,
    CTL_SRC_CD AS SRC_CTL_SRC_CD
  
  FROM `15_RDV_POLmp_rdv_H_REF_S_POL_MISS_v_POL_MISS34` AS in0

),

Get_RDV_Sort_by_Hash_EXPR_245 AS (

  SELECT REF_HK AS RDV_REF_HK
  
  FROM `15_RDV_POLmp_rdv_H_REF_S_POL_MISS_H_REF33` AS in0

),

Left_Outer_Join_by_Hash_Source_On_Left AS (

  SELECT 
    in0.SRC_REF_HK AS SRC_REF_HK,
    in0.SRC_REF_BK AS SRC_REF_BK,
    in0.SRC_CTL_SRC_CD AS SRC_CTL_SRC_CD,
    in1.RDV_REF_HK AS RDV_REF_HK
  
  FROM Remove_Duplicates_EXPR_244 AS in0
  LEFT JOIN Get_RDV_Sort_by_Hash_EXPR_245 AS in1
     ON (in0.SRC_REF_HK = in1.RDV_REF_HK)

),

Filter_New AS (

  SELECT * 
  
  FROM Left_Outer_Join_by_Hash_Source_On_Left AS in0
  
  WHERE CAST((RDV_REF_HK IS NULL) AS BOOLEAN)

),

Add_Control_Fields AS (

  SELECT 
    SRC_REF_HK AS REF_HK,
    SRC_REF_BK AS REF_BK,
    SRC_CTL_SRC_CD AS CTL_SRC_CD,
    CAST({{ var('CTL_BATCH_ID') }} AS INTEGER) AS CTL_BATCH_ID,
    {{ var('PMWorkflowRunId') }} AS CTL_JOB_ID,
    (TO_TIMESTAMP({{ var('CTL_LOAD_DM') }}, 'yyyy-MM-dd HH:mm:ss')) AS CTL_LOAD_DM
  
  FROM Filter_New AS in0

)

SELECT *

FROM Add_Control_Fields
