{{
  config({    
    "materialized": "table",
    "alias": "sh_DIM_GI_TRANSLATION",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_CLAIM_SK` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Translation', 
      '35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_CLAIM_SK'
    )
  }}

),

`35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_POLICY_RISK_SK` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Translation', 
      '35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_POLICY_RISK_SK'
    )
  }}

),

`35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_POLICY_SK` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Translation', 
      '35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_POLICY_SK'
    )
  }}

),

`35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_POLICY_SK` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Translation', 
      '35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_POLICY_SK'
    )
  }}

),

`35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_CLAIM_SK` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Translation', 
      '35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_CLAIM_SK'
    )
  }}

),

`35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_POLICY_RISK_SK` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Translation', 
      '35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_POLICY_RISK_SK'
    )
  }}

),

EXP_Hardcode_Values_EBIADM_Claim AS (

  SELECT 
    (
      CASE
        WHEN (BUSN_AREA_NAME = 'AMIA_NZ')
          THEN 'AMIA'
        ELSE BUSN_AREA_NAME
      END
    ) AS BUSN_AREA_NAME,
    CLAIM_NUM AS CLAIM_NUM,
    CLAIM_ID AS CLAIM_ID,
    (
      CASE
        WHEN ((BUSN_AREA_NAME IN ('AMIA_NZ', 'NZA')) = TRUE)
          THEN '5'
        WHEN (BUSN_AREA_NAME = 'AMIA')
          THEN '2'
        WHEN (BUSN_AREA_NAME = 'GI')
          THEN CMPY_NUM
        ELSE NULL
      END
    ) AS CMPY_NUM
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_CLAIM_SK` AS in0

),

SRT_EBIADM_Claim_EXPR_249 AS (

  SELECT 
    CLAIM_ID AS CLAIM_ID1,
    CLAIM_NUM AS CLAIM_NUM1,
    CMPY_NUM AS CMPY_NUM1,
    BUSN_AREA_NAME AS BUSN_AREA_NAME1
  
  FROM EXP_Hardcode_Values_EBIADM_Claim AS in0

),

JNR_Claim_ID AS (

  SELECT 
    in0.CMPY_NUM1 AS CMPY_NUM1,
    in0.BUSN_AREA_NAME1 AS BUSN_AREA_NAME1,
    in0.CLAIM_NUM1 AS CLAIM_NUM1,
    in1.CLAIM_ID AS CLAIM_ID,
    in1.CMPY_NUM AS CMPY_NUM,
    in1.CLAIM_NUM AS CLAIM_NUM,
    in1.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in0.CLAIM_ID1 AS CLAIM_ID1
  
  FROM SRT_EBIADM_Claim_EXPR_249 AS in0
  INNER JOIN `35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_CLAIM_SK` AS in1
     ON (
      ((in0.CLAIM_NUM1 = in1.CLAIM_NUM) AND (in0.CMPY_NUM1 = in1.CMPY_NUM))
      AND (in0.BUSN_AREA_NAME1 = in1.BUSN_AREA_NAME)
    )

),

EXP_Derived_Claim_ID AS (

  SELECT 
    CLAIM_ID1 AS EBIADM_ID1,
    CLAIM_ID AS MODEL_ID1,
    'CLAIM_ID' AS TRANS_TYPE1,
    CURRENT_TIMESTAMP AS CREATE_DATE1
  
  FROM JNR_Claim_ID AS in0

),

UNI_Trans_Type_EXPR_CLAIM_ID AS (

  SELECT 
    EBIADM_ID1 AS EBIADM_ID,
    MODEL_ID1 AS MODEL_ID,
    TRANS_TYPE1 AS TRANS_TYPE,
    CREATE_DATE1 AS CREATE_DATE
  
  FROM EXP_Derived_Claim_ID AS in0

),

SRT_MODEL_Policy_EXPR_248 AS (

  SELECT 
    POL_ID AS POL_ID1,
    POL_NUM AS POL_NUM1,
    BUSN_AREA_NAME AS BUSN_AREA_NAME1,
    PRD_CODE AS PRD_CODE1,
    CMPY_NUM AS CMPY_NUM1
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_POLICY_SK` AS in0

),

EXP_Hardcode_Values_EBIADM_Policy AS (

  SELECT 
    (
      CASE
        WHEN (BUSN_AREA_NAME = 'AMIA_NZ')
          THEN 'AMIA'
        ELSE BUSN_AREA_NAME
      END
    ) AS BUSN_AREA_NAME,
    (
      CASE
        WHEN ((BUSN_AREA_NAME IN ('AMIA_NZ', 'NZA')) = TRUE)
          THEN '5'
        WHEN (BUSN_AREA_NAME = 'AMIA')
          THEN '2'
        WHEN (BUSN_AREA_NAME = 'GI')
          THEN CMPY_NUM
        ELSE NULL
      END
    ) AS CMPY_NUM,
    POL_NUM AS POL_NUM,
    PRD_CODE AS PRD_CODE,
    POL_ID AS POL_ID
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_POLICY_SK` AS in0

),

JNR_Policy_ID AS (

  SELECT 
    in0.CMPY_NUM1 AS CMPY_NUM1,
    in0.BUSN_AREA_NAME1 AS BUSN_AREA_NAME1,
    in1.CMPY_NUM AS CMPY_NUM,
    in1.POL_NUM AS POL_NUM,
    in0.POL_ID1 AS POL_ID1,
    in1.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in0.POL_NUM1 AS POL_NUM1,
    in1.POL_ID AS POL_ID,
    in0.PRD_CODE1 AS PRD_CODE1,
    in1.PRD_CODE AS PRD_CODE
  
  FROM SRT_MODEL_Policy_EXPR_248 AS in0
  INNER JOIN EXP_Hardcode_Values_EBIADM_Policy AS in1
     ON (
      (
        ((in0.POL_NUM1 = in1.POL_NUM) AND (in0.BUSN_AREA_NAME1 = in1.BUSN_AREA_NAME))
        AND (in0.PRD_CODE1 = in1.PRD_CODE)
      )
      AND (in0.CMPY_NUM1 = in1.CMPY_NUM)
    )

),

EXP_Derived_Policy_ID AS (

  SELECT 
    POL_ID AS EBIADM_ID2,
    POL_ID1 AS MODEL_ID2,
    'POL_ID' AS TRANS_TYPE2,
    CURRENT_TIMESTAMP AS CREATE_DATE2
  
  FROM JNR_Policy_ID AS in0

),

UNI_Trans_Type_EXPR_POL_ID AS (

  SELECT 
    EBIADM_ID2 AS EBIADM_ID,
    MODEL_ID2 AS MODEL_ID,
    TRANS_TYPE2 AS TRANS_TYPE,
    CREATE_DATE2 AS CREATE_DATE
  
  FROM EXP_Derived_Policy_ID AS in0

),

EXP_Derived_MODEL_Rsk_Num AS (

  SELECT 
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    CMPY_CODE AS CMPY_CODE,
    (
      CASE
        WHEN (RSK_NUM = -1)
          THEN ABS(RSK_NUM)
        WHEN (RSK_NUM = 0)
          THEN 1
        ELSE RSK_NUM
      END
    ) AS RSK_NUM,
    RSK_ID AS RSK_ID,
    POL_NUM AS POL_NUM,
    PRD_CODE AS PRD_CODE
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_POLICY_RISK_SK` AS in0

),

EXP_Hardcode_Values_EBIADM_Risk AS (

  SELECT 
    RISK_ID AS RISK_ID,
    RISK_NUM AS RISK_NUM,
    (
      CASE
        WHEN (BUSN_AREA_NAME = 'AMIA_NZ')
          THEN 'AMIA'
        ELSE BUSN_AREA_NAME
      END
    ) AS BUSN_AREA_NAME,
    (
      CASE
        WHEN CAST((BUSN_AREA_NAME IN ('AMIA_NZ', 'NZA')) AS BOOLEAN)
          THEN '5'
        WHEN (BUSN_AREA_NAME = 'AMIA')
          THEN '2'
        WHEN (BUSN_AREA_NAME = 'GI')
          THEN CMPY_NUM
        ELSE NULL
      END
    ) AS CMPY_NUM,
    POL_NUM AS POL_NUM,
    PRD_CODE AS PRD_CODE
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Translation_SQ_sh_DIM_GI_POLICY_RISK_SK` AS in0

),

SRT_EBIADM_Risk_EXPR_253 AS (

  SELECT 
    CMPY_NUM AS CMPY_NUM_ebiadm,
    POL_NUM AS POL_NUM_ebiadm,
    PRD_CODE AS PRD_CODE_ebiadm,
    RISK_ID AS RISK_ID_ebiadm,
    RISK_NUM AS RISK_NUM_ebiadm,
    BUSN_AREA_NAME AS BUSN_AREA_NAME_ebiadm
  
  FROM EXP_Hardcode_Values_EBIADM_Risk AS in0

),

JNR_Risk_ID AS (

  SELECT 
    in0.PRD_CODE_ebiadm AS PRD_CODE_ebiadm,
    in0.RISK_NUM_ebiadm AS RISK_NUM_ebiadm,
    in0.RISK_ID_ebiadm AS RISK_ID_ebiadm,
    in1.POL_NUM AS POL_NUM,
    in0.POL_NUM_ebiadm AS POL_NUM_ebiadm,
    in0.BUSN_AREA_NAME_ebiadm AS BUSN_AREA_NAME_ebiadm,
    in1.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in1.RSK_ID AS RSK_ID,
    in1.RSK_NUM AS RSK_NUM,
    in0.CMPY_NUM_ebiadm AS CMPY_NUM_ebiadm,
    in1.CMPY_CODE AS CMPY_CODE,
    in1.PRD_CODE AS PRD_CODE
  
  FROM SRT_EBIADM_Risk_EXPR_253 AS in0
  INNER JOIN EXP_Derived_MODEL_Rsk_Num AS in1
     ON (
      (
        (
          ((in1.RSK_NUM = in0.RISK_NUM_ebiadm) AND (in1.BUSN_AREA_NAME = in0.BUSN_AREA_NAME_ebiadm))
          AND (in1.CMPY_CODE = in0.CMPY_NUM_ebiadm)
        )
        AND (in1.PRD_CODE = in0.PRD_CODE_ebiadm)
      )
      AND (in1.POL_NUM = in0.POL_NUM_ebiadm)
    )

),

EXP_Derived_Risk_ID AS (

  SELECT 
    RISK_ID_ebiadm AS EBIADM_ID3,
    RSK_ID AS MODEL_ID3,
    'RISK_ID' AS TRANS_TYPE3,
    CURRENT_TIMESTAMP AS CREATE_DATE3
  
  FROM JNR_Risk_ID AS in0

),

UNI_Trans_Type_EXPR_RISK_ID AS (

  SELECT 
    EBIADM_ID3 AS EBIADM_ID,
    MODEL_ID3 AS MODEL_ID,
    TRANS_TYPE3 AS TRANS_TYPE,
    CREATE_DATE3 AS CREATE_DATE
  
  FROM EXP_Derived_Risk_ID AS in0

),

UNI_Trans_Type AS (

  {{
    prophecy_basics.UnionByName(
      ['UNI_Trans_Type_EXPR_CLAIM_ID', 'UNI_Trans_Type_EXPR_POL_ID', 'UNI_Trans_Type_EXPR_RISK_ID'], 
      [
        '[{"name": "EBIADM_ID", "dataType": "Integer"}, {"name": "MODEL_ID", "dataType": "Integer"}, {"name": "TRANS_TYPE", "dataType": "String"}, {"name": "CREATE_DATE", "dataType": "Timestamp"}]', 
        '[{"name": "EBIADM_ID", "dataType": "Integer"}, {"name": "MODEL_ID", "dataType": "Integer"}, {"name": "TRANS_TYPE", "dataType": "String"}, {"name": "CREATE_DATE", "dataType": "Timestamp"}]', 
        '[{"name": "EBIADM_ID", "dataType": "Integer"}, {"name": "MODEL_ID", "dataType": "Integer"}, {"name": "TRANS_TYPE", "dataType": "String"}, {"name": "CREATE_DATE", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM UNI_Trans_Type
