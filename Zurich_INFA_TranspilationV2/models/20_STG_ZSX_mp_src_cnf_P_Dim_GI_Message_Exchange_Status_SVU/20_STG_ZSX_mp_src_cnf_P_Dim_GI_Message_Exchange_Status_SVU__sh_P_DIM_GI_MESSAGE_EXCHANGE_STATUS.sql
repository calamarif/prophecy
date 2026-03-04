{{
  config({    
    "materialized": "table",
    "alias": "sh_P_DIM_GI_MESSAGE_EXCHANGE_STATUS",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `20_STG_ZSXmp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU_sh_ZSX_SVU_GTWY_MESSAGE_STATUS` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '20_STG_ZSX_mp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU', 
      '20_STG_ZSXmp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU_sh_ZSX_SVU_GTWY_MESSAGE_STATUS'
    )
  }}

),

`20_STG_ZSXmp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU_SOURCE_LKP_Zsx_Svu_Gtwy_Attachment` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '20_STG_ZSX_mp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU', 
      '20_STG_ZSXmp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU_SOURCE_LKP_Zsx_Svu_Gtwy_Attachment'
    )
  }}

),

SQ_sh_ZSX_SVU_GTWY_MESSAGE_STATUS AS (

  SELECT 
    ID AS ID,
    INTERNAL_ID AS INTERNAL_ID,
    STATUS AS STATUS,
    CREATED AS CREATED,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    (monotonically_increasing_id()) AS prophecy_sk,
    INTERNAL_ID AS in_INTERNAL_ID
  
  FROM `20_STG_ZSXmp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU_sh_ZSX_SVU_GTWY_MESSAGE_STATUS` AS in0

),

EXP_Set_Defaults AS (

  SELECT 
    'SVU' AS PARTNER_GATEWAY,
    (
      CASE
        WHEN ((STATUS IS NULL) OR (CAST((RTRIM((LTRIM(STATUS)))) AS string) = ''))
          THEN '-'
        ELSE STATUS
      END
    ) AS EXCHANGE_STATUS_CODE,
    prophecy_sk AS prophecy_sk
  
  FROM SQ_sh_ZSX_SVU_GTWY_MESSAGE_STATUS AS in0

),

EXP_Ctl_Fields_STG31 AS (

  SELECT 
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    {{ var('PMWorkflowRunId') }} AS CTL_JOB_ID,
    CURRENT_TIMESTAMP AS CTL_REC_CRTN_DATE,
    prophecy_sk AS prophecy_sk
  
  FROM SQ_sh_ZSX_SVU_GTWY_MESSAGE_STATUS AS in0

),

FIL_Only_With_Attachments_JOIN_merged AS (

  SELECT 
    in0.EXCHANGE_STATUS_CODE AS EXCHANGE_STATUS_CODE,
    in1.CTL_BATCH_ID AS CTL_BATCH_ID,
    in1.CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    in2.INTERNAL_ID AS INTERNAL_ID,
    in2.in_INTERNAL_ID AS in_INTERNAL_ID,
    in1.CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    in1.CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    in2.STATUS AS STATUS,
    in0.prophecy_sk AS prophecy_sk,
    in0.PARTNER_GATEWAY AS PARTNER_GATEWAY,
    in1.CTL_JOB_ID AS CTL_JOB_ID,
    in2.CREATED AS CREATED,
    in1.CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID
  
  FROM EXP_Set_Defaults AS in0
  INNER JOIN EXP_Ctl_Fields_STG31 AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)
  INNER JOIN SQ_sh_ZSX_SVU_GTWY_MESSAGE_STATUS AS in2
     ON (in1.prophecy_sk = in2.prophecy_sk)
  INNER JOIN `20_STG_ZSXmp_src_cnf_P_Dim_GI_Message_Exchange_Status_SVU_SOURCE_LKP_Zsx_Svu_Gtwy_Attachment` AS in3
     ON (in3.INTERNAL_ID = in2.in_INTERNAL_ID)
  INNER JOIN SQ_sh_ZSX_SVU_GTWY_MESSAGE_STATUS AS in4
     ON in2.prophecy_sk = in4.prophecy_sk

),

FIL_Only_With_Attachments_JOIN_EXPR_187 AS (

  SELECT 
    PARTNER_GATEWAY AS PARTNER_GATEWAY,
    EXCHANGE_STATUS_CODE AS EXCHANGE_STATUS_CODE,
    prophecy_sk AS prophecy_sk,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    INTERNAL_ID AS lk_INTERNAL_ID,
    CREATED AS REC_STRT_DATE,
    INTERNAL_ID AS INTERNAL_ID
  
  FROM FIL_Only_With_Attachments_JOIN_merged AS in0

),

FIL_Only_With_Attachments AS (

  SELECT * 
  
  FROM FIL_Only_With_Attachments_JOIN_EXPR_187 AS in0
  
  WHERE CAST(((lk_INTERNAL_ID IS NULL) = FALSE) AS BOOLEAN)

)

SELECT *

FROM FIL_Only_With_Attachments
