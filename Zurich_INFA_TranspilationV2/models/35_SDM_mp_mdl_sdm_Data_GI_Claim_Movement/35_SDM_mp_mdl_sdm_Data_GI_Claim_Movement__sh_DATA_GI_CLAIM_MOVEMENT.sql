{{
  config({    
    "materialized": "table",
    "alias": "sh_DATA_GI_CLAIM_MOVEMENT",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_All_Irrlvnt` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_All_Irrlvnt'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Branch_Id` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Branch_Id'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_sh_DATA_GI_CLAIM_TRANSACTION` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_sh_DATA_GI_CLAIM_TRANSACTION'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Exclude_MTRN_Type` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Exclude_MTRN_Type'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Prd_Id` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Prd_Id'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Client_Group` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Client_Group'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Client_Branch_Company` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Client_Branch_Company'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Branch_Company` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Branch_Company'
    )
  }}

),

SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_sk,
    *
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_sh_DATA_GI_CLAIM_TRANSACTION` AS in0

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_Client` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_Client'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_By_ClaimID` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_By_ClaimID'
    )
  }}

),

SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_EXPR_313 AS (

  SELECT 
    CLAIM_ID AS IN_CLAIM_ID,
    prophecy_sk AS prophecy_sk,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    CLAIM_CMPY_CODE AS CLAIM_CMPY_CODE,
    PRD_CODE AS PRD_CODE,
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    CLAIM_PAYMT_TYPE_CODE AS CLAIM_PAYMT_TYPE_CODE,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    RSK_CLASS_ID AS RSK_CLASS_ID,
    RI_METHOD_CODE AS RI_METHOD_CODE,
    TRANS_AMT AS TRANS_AMT,
    TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    TRANS_BATCH_TYPE AS TRANS_BATCH_TYPE,
    TRANS_TS AS TRANS_TS,
    ACCTING_MTH_ID AS ACCTING_MTH_ID,
    TRANS_CCY_CODE AS TRANS_CCY_CODE,
    TRANS_ID AS TRANS_ID,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    AMT_TYPE_CODE AS AMT_TYPE_CODE,
    TRANS_REC_TYPE AS TRANS_REC_TYPE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0

),

LKP_Dim_GI_Claim_By_ClaimID_JOIN AS (

  SELECT 
    in0.INVESTIGATOR_CODE AS INVESTIGATOR_CODE,
    in0.MSTR_CLAIM_NUM AS MSTR_CLAIM_NUM,
    in0.DATE_OF_STATUS AS DATE_OF_STATUS,
    in0.ICA_CAT_CODE AS ICA_CAT_CODE,
    in1.IN_CLAIM_ID AS IN_CLAIM_ID,
    in0.DATE_RPTED AS DATE_RPTED,
    in0.BODILY_INJURY_FLG AS BODILY_INJURY_FLG,
    in0.UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    in0.CLAIM_ID AS CLAIM_ID,
    in0.CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    in0.CMPY_NUM AS CMPY_NUM,
    in0.DEFAULT_PREM_CLASS_CODE AS DEFAULT_PREM_CLASS_CODE,
    in0.COVR_EXPIRY_DATE AS COVR_EXPIRY_DATE,
    in0.CLSD_DATE AS CLSD_DATE,
    in0.DATE_OF_LOSS AS DATE_OF_LOSS,
    in0.CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE,
    in0.SOLICITOR_CODE AS SOLICITOR_CODE,
    in0.CLAIM_NUM AS CLAIM_NUM,
    in0.CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    in1.prophecy_sk AS prophecy_sk,
    in0.ASSESSOR_CODE AS ASSESSOR_CODE,
    in0.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in0.COVR_INCEPTION_DATE AS COVR_INCEPTION_DATE,
    in0.RSK_ID AS RSK_ID,
    in0.CLAIM_BRANCH_CODE AS CLAIM_BRANCH_CODE,
    in0.CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    in0.CLAIM_FRAUD_CODE AS CLAIM_FRAUD_CODE,
    in0.POL_ID AS POL_ID,
    in0.CLIENT_PARTY_ID AS CLIENT_PARTY_ID,
    in0.LOSS_PRD_CODE AS LOSS_PRD_CODE
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_By_ClaimID` AS in0
  INNER JOIN SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_EXPR_313 AS in1
     ON (in0.CLAIM_ID = in1.IN_CLAIM_ID)

),

LKP_Dim_GI_Claim_By_ClaimID_JOIN_EXPR_315 AS (

  SELECT 
    CLIENT_PARTY_ID AS IN_CLIENT_PARTY_ID,
    prophecy_sk AS prophecy_sk,
    IN_CLAIM_ID AS IN_CLAIM_ID,
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    RSK_ID AS RSK_ID,
    DATE_OF_STATUS AS DATE_OF_STATUS,
    LOSS_PRD_CODE AS LOSS_PRD_CODE,
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    INVESTIGATOR_CODE AS INVESTIGATOR_CODE,
    ASSESSOR_CODE AS ASSESSOR_CODE,
    SOLICITOR_CODE AS SOLICITOR_CODE,
    CLAIM_FRAUD_CODE AS CLAIM_FRAUD_CODE,
    DATE_RPTED AS DATE_RPTED,
    CLSD_DATE AS CLSD_DATE,
    DATE_OF_LOSS AS DATE_OF_LOSS,
    CLAIM_NUM AS CLAIM_NUM,
    CMPY_NUM AS CMPY_NUM,
    ICA_CAT_CODE AS ICA_CAT_CODE,
    BODILY_INJURY_FLG AS BODILY_INJURY_FLG,
    CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    CLAIM_ID AS CLAIM_ID,
    UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    MSTR_CLAIM_NUM AS MSTR_CLAIM_NUM,
    DEFAULT_PREM_CLASS_CODE AS DEFAULT_PREM_CLASS_CODE,
    COVR_INCEPTION_DATE AS COVR_INCEPTION_DATE,
    COVR_EXPIRY_DATE AS COVR_EXPIRY_DATE,
    POL_ID AS POL_ID,
    CLAIM_BRANCH_CODE AS CLAIM_BRANCH_CODE,
    CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE
  
  FROM LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in0

),

sh_LKP_Dim_Client_JOIN AS (

  SELECT 
    in0.STAFF_FLG AS STAFF_FLG,
    in0.LAST_KNOWN_ADDR_FLG AS LAST_KNOWN_ADDR_FLG,
    in0.INPUT_TAX_CREDIT AS INPUT_TAX_CREDIT,
    in0.OVERSEAS_FLG AS OVERSEAS_FLG,
    in0.STATE_REGTRN_NUM AS STATE_REGTRN_NUM,
    in0.CLIENT_DEATH_FLG AS CLIENT_DEATH_FLG,
    in0.RNWL_INVITATION_DELIVERY_EMAIL_ADDR AS RNWL_INVITATION_DELIVERY_EMAIL_ADDR,
    in0.RNWL_INVITATION_DELIVERY_METHOD AS RNWL_INVITATION_DELIVERY_METHOD,
    in1.IN_CLIENT_PARTY_ID AS IN_CLIENT_PARTY_ID,
    in0.EXTNL_REFERENCE AS EXTNL_REFERENCE,
    in1.prophecy_sk AS prophecy_sk,
    in0.CLIENT_GROUP_CODE AS CLIENT_GROUP_CODE,
    in0.RSDNCY_CNTRY_CODE AS RSDNCY_CNTRY_CODE,
    in0.MLNG_SUPRS_FLG AS MLNG_SUPRS_FLG,
    in0.AGE_ADMSSN_FLG AS AGE_ADMSSN_FLG,
    in0.TAX_FILE_NUM_SUPRS_DISP_FLG AS TAX_FILE_NUM_SUPRS_DISP_FLG,
    in0.CLIENT_PARTY_ID AS CLIENT_PARTY_ID,
    in0.STATE_OF_REGISTRY AS STATE_OF_REGISTRY
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_Client` AS in0
  INNER JOIN LKP_Dim_GI_Claim_By_ClaimID_JOIN_EXPR_315 AS in1
     ON (in0.CLIENT_PARTY_ID = CAST(in1.IN_CLIENT_PARTY_ID AS INTEGER))

),

EXP_Get_DATE_Id_and_MTH_Id AS (

  SELECT 
    CAST((TO_TIMESTAMP(DATE_RPTED, 'yyyyMMdd')) AS INTEGER) AS RPTED_DATE_ID,
    CAST((TO_TIMESTAMP(DATE_RPTED, 'yyyyMM')) AS INTEGER) AS RPTED_MTH_ID,
    CAST((TO_TIMESTAMP(DATE_OF_LOSS, 'yyyyMMdd')) AS INTEGER) AS LOSS_DATE_ID,
    CAST((TO_TIMESTAMP(DATE_OF_LOSS, 'yyyyMM')) AS INTEGER) AS LOSS_MTH_ID,
    prophecy_sk AS prophecy_sk
  
  FROM LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in0

),

EXP_Set_Lookup_Inputs_JOIN AS (

  SELECT 
    in0.CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    in3.LOSS_DATE_ID AS LOSS_DATE_ID,
    in0.CLAIM_CMPY_CODE AS CLAIM_CMPY_CODE,
    in0.prophecy_sk AS prophecy_sk,
    in2.CLIENT_GROUP_CODE AS CLIENT_GROUP_CODE,
    in1.CLAIM_BRANCH_CODE AS CLAIM_BRANCH_CODE
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0
  INNER JOIN LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)
  INNER JOIN sh_LKP_Dim_Client_JOIN AS in2
     ON (in1.prophecy_sk = in2.prophecy_sk)
  INNER JOIN EXP_Get_DATE_Id_and_MTH_Id AS in3
     ON (in2.prophecy_sk = in3.prophecy_sk)

),

EXP_Set_Lookup_Inputs AS (

  SELECT 
    CTL_SRC_SYS_SET_NAME AS Src_Sys_Name,
    '-' AS Dash,
    CLAIM_BRANCH_CODE AS Claim_Branch_Num,
    CLAIM_CMPY_CODE AS Claim_Cmpy_Code,
    CLIENT_GROUP_CODE AS Client_Group_Code,
    LOSS_DATE_ID AS Claim_Loss_Start_Date_ID,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Set_Lookup_Inputs_JOIN AS in0

),

EXP_Set_Lookup_Inputs_EXPR_300 AS (

  SELECT 
    Claim_Loss_Start_Date_ID AS IN_LOSS_DATE_ID,
    Dash AS IN_CLAIM_BRANCH_CODE,
    Src_Sys_Name AS IN_SRC_NAME,
    Client_Group_Code AS IN_CLIENT_GROUP_CODE,
    Dash AS IN_CLAIM_CMPY_CODE,
    prophecy_sk AS prophecy_sk,
    Claim_Branch_Num AS Claim_Branch_Num,
    Claim_Cmpy_Code AS Claim_Cmpy_Code
  
  FROM EXP_Set_Lookup_Inputs AS in0

),

EXP_Set_Lookup_Inputs_EXPR_299 AS (

  SELECT 
    Claim_Branch_Num AS IN_CLAIM_BRANCH_CODE,
    Dash AS IN_CLIENT_GROUP_CODE,
    Src_Sys_Name AS IN_SRC_NAME,
    Claim_Cmpy_Code AS IN_CLAIM_CMPY_CODE,
    Claim_Loss_Start_Date_ID AS IN_LOSS_DATE_ID,
    prophecy_sk AS prophecy_sk,
    Client_Group_Code AS Client_Group_Code
  
  FROM EXP_Set_Lookup_Inputs AS in0

),

EXP_Set_Lookup_Inputs_EXPR_298 AS (

  SELECT 
    Claim_Loss_Start_Date_ID AS IN_LOSS_DATE_ID,
    Dash AS IN_CLAIM_BRANCH_CODE,
    Src_Sys_Name AS IN_SRC_NAME,
    Dash AS IN_CLIENT_GROUP_CODE,
    Dash AS IN_CLAIM_CMPY_CODE,
    prophecy_sk AS prophecy_sk,
    Claim_Branch_Num AS Claim_Branch_Num,
    Claim_Cmpy_Code AS Claim_Cmpy_Code,
    Client_Group_Code AS Client_Group_Code
  
  FROM EXP_Set_Lookup_Inputs AS in0

),

EXP_Pick_Segment_1_JOIN_merged AS (

  SELECT 
    in1.SELECTION_PREF AS SELECTION_PREF,
    in0.IN_LOSS_DATE_ID AS IN_LOSS_DATE_ID,
    in1.CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    in1.SEGMENT_CODE AS SEGMENT_CODE,
    in0.IN_SRC_NAME AS IN_SRC_NAME,
    in1.CLAIM_BRANCH_NUM AS CLAIM_BRANCH_NUM,
    in0.IN_CLAIM_BRANCH_CODE AS IN_CLAIM_BRANCH_CODE,
    in1.STRT_LOSS_DATE AS STRT_LOSS_DATE,
    in1.CLAIM_CMPY_CODE AS CLAIM_CMPY_CODE,
    in0.prophecy_sk AS prophecy_sk,
    in1.END_LOSS_DATE AS END_LOSS_DATE,
    in1.CLIENT_GROUP_CODE AS CLIENT_GROUP_CODE,
    in0.IN_CLAIM_CMPY_CODE AS IN_CLAIM_CMPY_CODE,
    in0.IN_CLIENT_GROUP_CODE AS IN_CLIENT_GROUP_CODE
  
  FROM EXP_Set_Lookup_Inputs_EXPR_300 AS in0
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Branch_Company` AS in1
     ON (
      (
        (
          (
            ((in1.CTL_SRC_SYS_SET_NAME = in0.IN_SRC_NAME) AND (in1.CLAIM_BRANCH_NUM = in0.IN_CLAIM_BRANCH_CODE))
            AND (in1.CLIENT_GROUP_CODE = in0.IN_CLIENT_GROUP_CODE)
          )
          AND (in1.CLAIM_CMPY_CODE = in0.IN_CLAIM_CMPY_CODE)
        )
        AND (in1.STRT_LOSS_DATE <= CAST(in0.IN_LOSS_DATE_ID AS INTEGER))
      )
      AND (in1.END_LOSS_DATE >= CAST(in0.IN_LOSS_DATE_ID AS INTEGER))
    )
  INNER JOIN EXP_Set_Lookup_Inputs_EXPR_298 AS in2
     ON in0.prophecy_sk = in2.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Client_Branch_Company` AS in3
     ON (
      (
        (
          (
            ((in3.CTL_SRC_SYS_SET_NAME = in2.IN_SRC_NAME) AND (in3.CLAIM_BRANCH_NUM = in2.IN_CLAIM_BRANCH_CODE))
            AND (in3.CLIENT_GROUP_CODE = in2.IN_CLIENT_GROUP_CODE)
          )
          AND (in3.CLAIM_CMPY_CODE = in2.IN_CLAIM_CMPY_CODE)
        )
        AND (in3.STRT_LOSS_DATE <= CAST(in2.IN_LOSS_DATE_ID AS INTEGER))
      )
      AND (in3.END_LOSS_DATE >= CAST(in2.IN_LOSS_DATE_ID AS INTEGER))
    )
  INNER JOIN EXP_Set_Lookup_Inputs_EXPR_299 AS in4
     ON in2.prophecy_sk = in4.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_Dim_GI_Claim_Segment_Mapping_Client_Group` AS in5
     ON (
      (
        (
          (
            ((in5.CTL_SRC_SYS_SET_NAME = in4.IN_SRC_NAME) AND (in5.CLAIM_BRANCH_NUM = in4.IN_CLAIM_BRANCH_CODE))
            AND (in5.CLIENT_GROUP_CODE = in4.IN_CLIENT_GROUP_CODE)
          )
          AND (in5.CLAIM_CMPY_CODE = in4.IN_CLAIM_CMPY_CODE)
        )
        AND (in5.STRT_LOSS_DATE <= CAST(in4.IN_LOSS_DATE_ID AS INTEGER))
      )
      AND (in5.END_LOSS_DATE >= CAST(in4.IN_LOSS_DATE_ID AS INTEGER))
    )

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Client_Group_Id` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Client_Group_Id'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_STATE_default_value` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_STATE_default_value'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Accessor` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Accessor'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_MTRN_Typ_Irrlvnt_Prem_Class` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_MTRN_Typ_Irrlvnt_Prem_Class'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Irrlvnt_MTRN_Typ_PREM_Class` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Irrlvnt_MTRN_Typ_PREM_Class'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Risk_Premium_Cover` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Risk_Premium_Cover'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Policy_Agent` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Policy_Agent'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Claim_Payment_Type` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Claim_Payment_Type'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_V_Dim_GI_Prem_Class` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_V_Dim_GI_Prem_Class'
    )
  }}

),

EXP_Get_Default_Prem_Class_If_Not_Present_JOIN AS (

  SELECT 
    in0.DEFAULT_PREM_CLASS_CODE AS DEFAULT_PREM_CLASS_CODE,
    in1.PREM_CLASS_CODE AS PREM_CLASS_CODE,
    in0.prophecy_sk AS prophecy_sk
  
  FROM LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in0
  INNER JOIN SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

EXP_Get_Default_Prem_Class_If_Not_Present AS (

  SELECT 
    (
      CASE
        WHEN (PREM_CLASS_CODE = '-')
          THEN DEFAULT_PREM_CLASS_CODE
        ELSE PREM_CLASS_CODE
      END
    ) AS PREM_CLASS_CODE_OUT,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Get_Default_Prem_Class_If_Not_Present_JOIN AS in0

),

sh_LKP_V_Dim_GI_Prem_Class_PRE_JOIN_JOIN AS (

  SELECT 
    in0.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in1.PREM_CLASS_CODE_OUT AS PREM_CLASS_CODE_OUT,
    in0.prophecy_sk AS prophecy_sk
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0
  INNER JOIN EXP_Get_Default_Prem_Class_If_Not_Present AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

sh_LKP_V_Dim_GI_Prem_Class_PRE_JOIN_JOIN_EXPR_288 AS (

  SELECT 
    BUSN_AREA_NAME AS IN_BUSN_AREA,
    prophecy_sk AS prophecy_sk,
    PREM_CLASS_CODE_OUT AS IN_PREMIUM_CLASS
  
  FROM sh_LKP_V_Dim_GI_Prem_Class_PRE_JOIN_JOIN AS in0

),

sh_LKP_V_Dim_GI_Prem_Class_JOIN AS (

  SELECT 
    in0.PREM_CLASS_DESC AS PREM_CLASS_DESC,
    in1.IN_BUSN_AREA AS IN_BUSN_AREA,
    in0.PREM_CLASS_ID AS PREM_CLASS_ID,
    in1.IN_PREMIUM_CLASS AS IN_PREMIUM_CLASS,
    in0.PREM_CLASS_CODE AS PREM_CLASS_CODE,
    in1.prophecy_sk AS prophecy_sk,
    in0.BUSN_AREA_NAME AS BUSN_AREA_NAME
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_V_Dim_GI_Prem_Class` AS in0
  INNER JOIN sh_LKP_V_Dim_GI_Prem_Class_PRE_JOIN_JOIN_EXPR_288 AS in1
     ON ((in0.BUSN_AREA_NAME = in1.IN_BUSN_AREA) AND (in0.PREM_CLASS_CODE = in1.IN_PREMIUM_CLASS))

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION_Payment_Typ_Code` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION_Payment_Typ_Code'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION_Prem_Class_Code` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION_Prem_Class_Code'
    )
  }}

),

EXP_Pass_Through_JOIN AS (

  SELECT 
    in0.CLAIM_ID AS CLAIM_ID,
    in0.PRD_CODE AS PRD_CODE,
    in1.BODILY_INJURY_FLG AS BODILY_INJURY_FLG,
    in0.prophecy_sk AS prophecy_sk
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0
  INNER JOIN LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

EXP_Pass_Through_JOIN_EXPR_287 AS (

  SELECT 
    CLAIM_ID AS CLAIM_ID,
    PRD_CODE AS PRODUCT_CODE,
    prophecy_sk AS prophecy_sk,
    BODILY_INJURY_FLG AS BODILY_INJURY_FLAG,
    CLAIM_ID AS in_CLAIM_ID
  
  FROM EXP_Pass_Through_JOIN AS in0

),

EXP_Pass_Through_EXPR_308 AS (

  SELECT 
    CLAIM_ID AS in_CLAIM_ID,
    prophecy_sk AS prophecy_sk,
    BODILY_INJURY_FLAG AS BODILY_INJURY_FLAG,
    PRODUCT_CODE AS PRODUCT_CODE
  
  FROM EXP_Pass_Through_JOIN_EXPR_287 AS in0

),

EXP_Calculate_Bodily_Injury_Code_JOIN_merged AS (

  SELECT 
    in0.in_CLAIM_ID AS in_CLAIM_ID,
    in0.CLAIM_ID AS CLAIM_ID,
    in0.BODILY_INJURY_FLAG AS BODILY_INJURY_FLAG,
    in0.PRODUCT_CODE AS PRODUCT_CODE,
    in0.prophecy_sk AS prophecy_sk
  
  FROM EXP_Pass_Through_JOIN_EXPR_287 AS in0
  INNER JOIN EXP_Pass_Through_JOIN_EXPR_287 AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION_Prem_Class_Code` AS in2
     ON (in2.CLAIM_ID = in1.in_CLAIM_ID)
  INNER JOIN EXP_Pass_Through_EXPR_308 AS in3
     ON in1.prophecy_sk = in3.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION_Payment_Typ_Code` AS in4
     ON (in4.CLAIM_ID = in3.in_CLAIM_ID)

),

EXP_Calculate_Bodily_Injury_Code AS (

  SELECT 
    (
      CASE
        WHEN (BODILY_INJURY_FLAG = 'Y')
          THEN 'A'
        WHEN (PRODUCT_CODE = 'CTP')
          THEN 'B'
        WHEN (PRODUCT_CODE = 'GWC')
          THEN 'C'
        WHEN ((PRODUCT_CODE = 'CCI') AND (NOT(CLAIM_ID IS NULL)))
          THEN 'D'
        WHEN (NOT(CLAIM_ID IS NULL))
          THEN 'E'
        ELSE '-'
      END
    ) AS BODILY_INJURY_CODE,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Calculate_Bodily_Injury_Code_JOIN_merged AS in0

),

LKP_Dim_GI_Claim_By_ClaimID_JOIN_EXPR_311 AS (

  SELECT 
    DATE_OF_LOSS AS IN_DATE_OF_LOSS,
    RSK_ID AS IN_RSK_ID,
    prophecy_sk AS prophecy_sk,
    IN_CLAIM_ID AS IN_CLAIM_ID,
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    DATE_OF_STATUS AS DATE_OF_STATUS,
    LOSS_PRD_CODE AS LOSS_PRD_CODE,
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    INVESTIGATOR_CODE AS INVESTIGATOR_CODE,
    ASSESSOR_CODE AS ASSESSOR_CODE,
    SOLICITOR_CODE AS SOLICITOR_CODE,
    CLAIM_FRAUD_CODE AS CLAIM_FRAUD_CODE,
    DATE_RPTED AS DATE_RPTED,
    CLSD_DATE AS CLSD_DATE,
    CLAIM_NUM AS CLAIM_NUM,
    CMPY_NUM AS CMPY_NUM,
    ICA_CAT_CODE AS ICA_CAT_CODE,
    BODILY_INJURY_FLG AS BODILY_INJURY_FLG,
    CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    CLAIM_ID AS CLAIM_ID,
    UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    MSTR_CLAIM_NUM AS MSTR_CLAIM_NUM,
    DEFAULT_PREM_CLASS_CODE AS DEFAULT_PREM_CLASS_CODE,
    COVR_INCEPTION_DATE AS COVR_INCEPTION_DATE,
    COVR_EXPIRY_DATE AS COVR_EXPIRY_DATE,
    POL_ID AS POL_ID,
    CLAIM_BRANCH_CODE AS CLAIM_BRANCH_CODE,
    CLIENT_PARTY_ID AS CLIENT_PARTY_ID,
    CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE
  
  FROM LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in0

),

LKP_Dim_GI_Claim_By_ClaimID_JOIN_EXPR_314 AS (

  SELECT 
    POL_ID AS IN_POL_ID,
    prophecy_sk AS prophecy_sk,
    IN_CLAIM_ID AS IN_CLAIM_ID,
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    RSK_ID AS RSK_ID,
    DATE_OF_STATUS AS DATE_OF_STATUS,
    LOSS_PRD_CODE AS LOSS_PRD_CODE,
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    INVESTIGATOR_CODE AS INVESTIGATOR_CODE,
    ASSESSOR_CODE AS ASSESSOR_CODE,
    SOLICITOR_CODE AS SOLICITOR_CODE,
    CLAIM_FRAUD_CODE AS CLAIM_FRAUD_CODE,
    DATE_RPTED AS DATE_RPTED,
    CLSD_DATE AS CLSD_DATE,
    DATE_OF_LOSS AS DATE_OF_LOSS,
    CLAIM_NUM AS CLAIM_NUM,
    CMPY_NUM AS CMPY_NUM,
    ICA_CAT_CODE AS ICA_CAT_CODE,
    BODILY_INJURY_FLG AS BODILY_INJURY_FLG,
    CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    CLAIM_ID AS CLAIM_ID,
    UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    MSTR_CLAIM_NUM AS MSTR_CLAIM_NUM,
    DEFAULT_PREM_CLASS_CODE AS DEFAULT_PREM_CLASS_CODE,
    COVR_INCEPTION_DATE AS COVR_INCEPTION_DATE,
    COVR_EXPIRY_DATE AS COVR_EXPIRY_DATE,
    CLAIM_BRANCH_CODE AS CLAIM_BRANCH_CODE,
    CLIENT_PARTY_ID AS CLIENT_PARTY_ID,
    CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE
  
  FROM LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in0

),

sh_LKP_Dim_GI_Accessor_PRE_JOIN_JOIN AS (

  SELECT 
    in0.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in1.ASSESSOR_CODE AS ASSESSOR_CODE,
    in0.prophecy_sk AS prophecy_sk
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0
  INNER JOIN LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

sh_LKP_Dim_GI_Accessor_PRE_JOIN_JOIN_EXPR_284 AS (

  SELECT 
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    prophecy_sk AS prophecy_sk,
    ASSESSOR_CODE AS IN_ITEM_CODE
  
  FROM sh_LKP_Dim_GI_Accessor_PRE_JOIN_JOIN AS in0

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_V_Dim_GI_Product` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_V_Dim_GI_Product'
    )
  }}

),

SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_EXPR_312 AS (

  SELECT 
    PRD_CODE AS IN_PRODUCT_CODE,
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    prophecy_sk AS prophecy_sk,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    CLAIM_CMPY_CODE AS CLAIM_CMPY_CODE,
    CLAIM_ID AS CLAIM_ID,
    CLAIM_PAYMT_TYPE_CODE AS CLAIM_PAYMT_TYPE_CODE,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    RSK_CLASS_ID AS RSK_CLASS_ID,
    RI_METHOD_CODE AS RI_METHOD_CODE,
    TRANS_AMT AS TRANS_AMT,
    TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    TRANS_BATCH_TYPE AS TRANS_BATCH_TYPE,
    TRANS_TS AS TRANS_TS,
    ACCTING_MTH_ID AS ACCTING_MTH_ID,
    TRANS_CCY_CODE AS TRANS_CCY_CODE,
    TRANS_ID AS TRANS_ID,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    AMT_TYPE_CODE AS AMT_TYPE_CODE,
    TRANS_REC_TYPE AS TRANS_REC_TYPE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0

),

sh_LKP_V_Dim_GI_Product_JOIN AS (

  SELECT 
    in0.PRODUCT_CODE AS PRODUCT_CODE,
    in0.PRODUCT_DESC AS PRODUCT_DESC,
    in1.prophecy_sk AS prophecy_sk,
    in1.IN_PRODUCT_CODE AS IN_PRODUCT_CODE,
    in0.PRODUCT_ID AS PRODUCT_ID,
    in0.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in1.IN_BUSN_AREA_NAME AS IN_BUSN_AREA_NAME
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_V_Dim_GI_Product` AS in0
  INNER JOIN SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_EXPR_312 AS in1
     ON ((in0.BUSN_AREA_NAME = in1.IN_BUSN_AREA_NAME) AND (in0.PRODUCT_CODE = in1.IN_PRODUCT_CODE))

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SUBSEGMENT_default_value` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SUBSEGMENT_default_value'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_default_value` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_default_value'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_COMMON_HIERARCHY` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_COMMON_HIERARCHY'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Prem_Class_Id` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Prem_Class_Id'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_BRANCH` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_BRANCH'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Client_Group_Code` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Client_Group_Code'
    )
  }}

),

sh_LKP_Client_Group_Code_PRE_JOIN_JOIN AS (

  SELECT 
    in0.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in1.CLIENT_GROUP_CODE AS CLIENT_GROUP_CODE,
    in0.prophecy_sk AS prophecy_sk
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0
  INNER JOIN sh_LKP_Dim_Client_JOIN AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

sh_LKP_Client_Group_Code_PRE_JOIN_JOIN_EXPR_285 AS (

  SELECT 
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    prophecy_sk AS prophecy_sk,
    CLIENT_GROUP_CODE AS IN_CLIENT_GROUP_CODE
  
  FROM sh_LKP_Client_Group_Code_PRE_JOIN_JOIN AS in0

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Branch` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Branch'
    )
  }}

),

sh_LKP_Dim_GI_Branch_PRE_JOIN_JOIN AS (

  SELECT 
    in0.CMPY_NUM AS CMPY_NUM,
    in0.CLAIM_BRANCH_CODE AS CLAIM_BRANCH_CODE,
    in1.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in0.prophecy_sk AS prophecy_sk
  
  FROM LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in0
  INNER JOIN SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

sh_LKP_Dim_GI_Branch_PRE_JOIN_JOIN_EXPR_286 AS (

  SELECT 
    CMPY_NUM AS IN_CMPY_CODE,
    CLAIM_BRANCH_CODE AS IN_BRANCH_CODE,
    prophecy_sk AS prophecy_sk,
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME
  
  FROM sh_LKP_Dim_GI_Branch_PRE_JOIN_JOIN AS in0

),

sh_LKP_Dim_GI_Branch_JOIN AS (

  SELECT 
    in1.IN_BRANCH_CODE AS IN_BRANCH_CODE,
    in0.DISP_ITEM_DESC AS DISP_ITEM_DESC,
    in0.ITEM_LONG_DESC AS ITEM_LONG_DESC,
    in0.REFERENCE_ID AS REFERENCE_ID,
    in1.prophecy_sk AS prophecy_sk,
    in0.ITEM_SHORT_DESC AS ITEM_SHORT_DESC,
    in1.IN_CMPY_CODE AS IN_CMPY_CODE,
    in0.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in1.IN_BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    in0.ITEM_CODE AS ITEM_CODE,
    in0.CMPY_CODE AS CMPY_CODE
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Branch` AS in0
  INNER JOIN sh_LKP_Dim_GI_Branch_PRE_JOIN_JOIN_EXPR_286 AS in1
     ON (
      ((in0.BUSN_AREA_NAME = in1.IN_BUSN_AREA_NAME) AND (in0.ITEM_CODE = in1.IN_BRANCH_CODE))
      AND (in0.CMPY_CODE = in1.IN_CMPY_CODE)
    )

),

EXP_JOIN_TYPE_JOIN_merged AS (

  SELECT 
    in1.DISP_ITEM_DESC AS DISP_ITEM_DESC,
    in1.ITEM_LONG_DESC AS ITEM_LONG_DESC,
    in4.PREM_CLASS_ID AS PREM_CLASS_ID,
    in1.REFERENCE_ID AS REFERENCE_ID,
    in0.prophecy_sk AS prophecy_sk,
    in1.ITEM_SHORT_DESC AS ITEM_SHORT_DESC,
    in3.PRODUCT_ID AS PRODUCT_ID,
    in1.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in0.IN_BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    in1.ITEM_CODE AS ITEM_CODE,
    in0.IN_CLIENT_GROUP_CODE AS IN_CLIENT_GROUP_CODE
  
  FROM sh_LKP_Client_Group_Code_PRE_JOIN_JOIN_EXPR_285 AS in0
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Client_Group_Code` AS in1
     ON ((in1.BUSN_AREA_NAME = in0.IN_BUSN_AREA_NAME) AND (in1.ITEM_CODE = in0.IN_CLIENT_GROUP_CODE))
  INNER JOIN sh_LKP_Dim_GI_Branch_JOIN AS in2
     ON in0.prophecy_sk = in2.prophecy_sk
  INNER JOIN sh_LKP_V_Dim_GI_Product_JOIN AS in3
     ON (in2.prophecy_sk = in3.prophecy_sk)
  INNER JOIN sh_LKP_V_Dim_GI_Prem_Class_JOIN AS in4
     ON (in3.prophecy_sk = in4.prophecy_sk)

),

EXP_JOIN_TYPE_JOIN_EXPR_282 AS (

  SELECT 
    REFERENCE_ID AS CLIENT_GROUP_ID,
    prophecy_sk AS prophecy_sk,
    REFERENCE_ID AS BRANCH_ID,
    PRODUCT_ID AS PRD_ID,
    PREM_CLASS_ID AS PREM_CLASS_ID,
    REFERENCE_ID AS BRANCH_ID_IN,
    PREM_CLASS_ID AS PREM_CLASS_ID_IN,
    PRODUCT_ID AS PRD_ID_IN,
    REFERENCE_ID AS in_BRANCH_ID,
    REFERENCE_ID AS CLIENT_GROUP_ID_IN
  
  FROM EXP_JOIN_TYPE_JOIN_merged AS in0

),

EXP_Pick_Segment_JOIN_merged AS (

  SELECT 
    in1._ORDER AS _ORDER,
    in0.in_BRANCH_ID AS in_BRANCH_ID,
    in0.BRANCH_ID AS BRANCH_ID,
    in0.PRD_ID_IN AS PRD_ID_IN,
    in1.SUBSEGMENT_ID AS SUBSEGMENT_ID,
    in0.CLIENT_GROUP_ID_IN AS CLIENT_GROUP_ID_IN,
    in0.PREM_CLASS_ID AS PREM_CLASS_ID,
    in0.CLIENT_GROUP_ID AS CLIENT_GROUP_ID,
    in0.PREM_CLASS_ID_IN AS PREM_CLASS_ID_IN,
    in0.BRANCH_ID_IN AS BRANCH_ID_IN,
    in0.prophecy_sk AS prophecy_sk,
    in0.PRD_ID AS PRD_ID,
    in7.STATE_ID AS STATE_ID
  
  FROM EXP_JOIN_TYPE_JOIN_EXPR_282 AS in0
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Client_Group_Id` AS in1
     ON (in1.CLIENT_GROUP_ID = in0.CLIENT_GROUP_ID_IN)
  INNER JOIN EXP_JOIN_TYPE_JOIN_EXPR_282 AS in2
     ON in0.prophecy_sk = in2.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Prem_Class_Id` AS in3
     ON (in3.PREM_CLASS_ID = in2.PREM_CLASS_ID_IN)
  INNER JOIN EXP_JOIN_TYPE_JOIN_EXPR_282 AS in4
     ON in2.prophecy_sk = in4.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Prd_Id` AS in5
     ON (in5.PRD_ID = in4.PRD_ID_IN)
  INNER JOIN EXP_JOIN_TYPE_JOIN_EXPR_282 AS in6
     ON in4.prophecy_sk = in6.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_BRANCH` AS in7
     ON (in7.BRANCH_ID = in6.in_BRANCH_ID)
  INNER JOIN EXP_JOIN_TYPE_JOIN_EXPR_282 AS in8
     ON in6.prophecy_sk = in8.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_MAPPING_By_Branch_Id` AS in9
     ON (in9.BRANCH_ID = in8.BRANCH_ID_IN)

),

EXP_Pick_Segment AS (

  SELECT 
    STATE_ID AS STATE_ID,
    79 AS HIERARCHY_TYPE_ID,
    (
      CASE
        WHEN (
          (
            (
              (
                CASE
                  WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                    THEN 2147000000
                  ELSE _ORDER
                END
              ) < (
                CASE
                  WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                    THEN 2147000000
                  ELSE _ORDER
                END
              )
            )
            AND (
                  (
                    CASE
                      WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                        THEN 2147000000
                      ELSE _ORDER
                    END
                  ) < (
                    CASE
                      WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                        THEN 2147000000
                      ELSE _ORDER
                    END
                  )
                )
          )
          AND (
                (
                  CASE
                    WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                      THEN 2147000000
                    ELSE _ORDER
                  END
                ) < (
                  CASE
                    WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                      THEN 2147000000
                    ELSE _ORDER
                  END
                )
              )
        )
          THEN SUBSEGMENT_ID
        WHEN (
          (
            (
              CASE
                WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                  THEN 2147000000
                ELSE _ORDER
              END
            ) < (
              CASE
                WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                  THEN 2147000000
                ELSE _ORDER
              END
            )
          )
          AND (
                (
                  CASE
                    WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                      THEN 2147000000
                    ELSE _ORDER
                  END
                ) < (
                  CASE
                    WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                      THEN 2147000000
                    ELSE _ORDER
                  END
                )
              )
        )
          THEN SUBSEGMENT_ID
        WHEN (
          (
            CASE
              WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                THEN 2147000000
              ELSE _ORDER
            END
          ) < (
            CASE
              WHEN CAST((_ORDER IS NULL) AS BOOLEAN)
                THEN 2147000000
              ELSE _ORDER
            END
          )
        )
          THEN SUBSEGMENT_ID
        ELSE SUBSEGMENT_ID
      END
    ) AS SUBSEGMENT_ID,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Pick_Segment_JOIN_merged AS in0

),

EXP_Pick_Segment_EXPR_301 AS (

  SELECT 
    SUBSEGMENT_ID AS FROM_REFERENCE_ID_in,
    HIERARCHY_TYPE_ID AS HIERARCHY_TYPE_ID_in,
    prophecy_sk AS prophecy_sk,
    STATE_ID AS STATE_ID
  
  FROM EXP_Pick_Segment AS in0

),

EXP_Default_Ids_JOIN_merged AS (

  SELECT 
    in2.HIERARCHY_TYPE_ID AS HIERARCHY_TYPE_ID,
    in1.HIERARCHY_TYPE_ID_in AS HIERARCHY_TYPE_ID_in,
    in2.FROM_REFERENCE_ID AS FROM_REFERENCE_ID,
    in0.SUBSEGMENT_ID AS SUBSEGMENT_ID,
    in2.FROM_ITEM_TABLE AS FROM_ITEM_TABLE,
    in2.TO_ITEM_TABLE AS TO_ITEM_TABLE,
    in0.prophecy_sk AS prophecy_sk,
    in2.TO_REFERENCE_ID AS TO_REFERENCE_ID,
    in1.FROM_REFERENCE_ID_in AS FROM_REFERENCE_ID_in,
    in0.STATE_ID AS STATE_ID
  
  FROM EXP_Pick_Segment AS in0
  INNER JOIN EXP_Pick_Segment_EXPR_301 AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_COMMON_HIERARCHY` AS in2
     ON (
      (in2.HIERARCHY_TYPE_ID = CAST(in1.HIERARCHY_TYPE_ID_in AS INTEGER))
      AND (in2.FROM_REFERENCE_ID = CAST(in1.FROM_REFERENCE_ID_in AS INTEGER))
    )

),

EXP_Default_Ids_JOIN_EXPR_279 AS (

  SELECT 
    SUBSEGMENT_ID AS SUBSEGMENT_ID_in,
    STATE_ID AS STATE_ID_in,
    prophecy_sk AS prophecy_sk,
    TO_REFERENCE_ID AS SEGMENT_ID_in
  
  FROM EXP_Default_Ids_JOIN_merged AS in0

),

EXP_Default_Ids_LOOKUP_merged AS (

  SELECT 
    in0.SEGMENT_ID AS LOOKUP_VARIABLE_1,
    in2.SUBSEGMENT_ID AS LOOKUP_VARIABLE_2,
    in3.STATE_ID AS LOOKUP_VARIABLE_3,
    in3.STATE_ID AS STATE_ID,
    in3.STATE_CODE AS STATE_CODE,
    in2.SUBSEGMENT_ID AS SUBSEGMENT_ID,
    in2.SUBSEGMENT_CODE AS SUBSEGMENT_CODE,
    in1.SUBSEGMENT_ID_in AS SUBSEGMENT_ID_in,
    in1.STATE_ID_in AS STATE_ID_in,
    in1.prophecy_sk AS prophecy_sk,
    in1.SEGMENT_ID_in AS SEGMENT_ID_in,
    in0.SEGMENT_ID AS SEGMENT_ID,
    in0.SEGMENT_CODE AS SEGMENT_CODE
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SEGMENT_default_value` AS in0
  LEFT JOIN EXP_Default_Ids_JOIN_EXPR_279 AS in1
     ON (in0.SEGMENT_CODE = 'UNKNOWN')
  RIGHT JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_SUBSEGMENT_default_value` AS in2
     ON (in2.SUBSEGMENT_CODE = 'UNKNOWN')
  RIGHT JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_STATE_default_value` AS in3
     ON (in3.STATE_CODE = '-')

),

EXP_Default_Ids AS (

  SELECT 
    (
      CASE
        WHEN CAST((SEGMENT_ID_in IS NULL) AS BOOLEAN)
          THEN LOOKUP_VARIABLE_1
        ELSE SEGMENT_ID_in
      END
    ) AS SEGMENT_ID,
    (
      CASE
        WHEN CAST((SUBSEGMENT_ID_in IS NULL) AS BOOLEAN)
          THEN LOOKUP_VARIABLE_2
        ELSE SUBSEGMENT_ID_in
      END
    ) AS SUBSEGMENT_ID,
    (
      CASE
        WHEN CAST((STATE_ID_in IS NULL) AS BOOLEAN)
          THEN LOOKUP_VARIABLE_3
        ELSE STATE_ID_in
      END
    ) AS STATE_ID,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Default_Ids_LOOKUP_merged AS in0

),

SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_EXPR_310 AS (

  SELECT 
    CLAIM_PAYMT_TYPE_CODE AS IN_CLAIM_PAYMENT_TYPE_CODE,
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    prophecy_sk AS prophecy_sk,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    CLAIM_CMPY_CODE AS CLAIM_CMPY_CODE,
    CLAIM_ID AS CLAIM_ID,
    PRD_CODE AS PRD_CODE,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    RSK_CLASS_ID AS RSK_CLASS_ID,
    RI_METHOD_CODE AS RI_METHOD_CODE,
    TRANS_AMT AS TRANS_AMT,
    TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    TRANS_BATCH_TYPE AS TRANS_BATCH_TYPE,
    TRANS_TS AS TRANS_TS,
    ACCTING_MTH_ID AS ACCTING_MTH_ID,
    TRANS_CCY_CODE AS TRANS_CCY_CODE,
    TRANS_ID AS TRANS_ID,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    AMT_TYPE_CODE AS AMT_TYPE_CODE,
    TRANS_REC_TYPE AS TRANS_REC_TYPE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in0

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_DIM_GI_REFERENCE` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_DIM_GI_REFERENCE'
    )
  }}

),

EXP_Pick_Segment_1_JOIN_EXPR_280 AS (

  SELECT 
    SELECTION_PREF AS SELECTION_PREF_Branch_Company,
    SEGMENT_CODE AS SEGMENT_CODE_Branch_Company,
    prophecy_sk AS prophecy_sk,
    SEGMENT_CODE AS SEGMENT_CODE_Client_Branch_Company,
    SELECTION_PREF AS SELECTION_PREF_Client_Branch_Company,
    SEGMENT_CODE AS SEGMENT_CODE_Client,
    SELECTION_PREF AS SELECTION_PREF_Client
  
  FROM EXP_Pick_Segment_1_JOIN_merged AS in0

),

EXP_Pick_Segment_1_LOOKUP_merged AS (

  SELECT 
    in0.REFERENCE_ID AS LOOKUP_VARIABLE_4,
    in2.REFERENCE_ID AS LOOKUP_VARIABLE_5,
    in3.REFERENCE_ID AS LOOKUP_VARIABLE_6,
    in3.REFERENCE_ID AS REFERENCE_ID,
    in3.VALID_FLG AS VALID_FLG,
    in3.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in3.ITEM_TABLE AS ITEM_TABLE,
    in3.CMPY_CODE AS CMPY_CODE,
    in3.ITEM_CODE AS ITEM_CODE,
    in3.DISP_ITEM_DESC AS DISP_ITEM_DESC,
    in3.ITEM_SHORT_DESC AS ITEM_SHORT_DESC,
    in3.ITEM_LONG_DESC AS ITEM_LONG_DESC,
    in1.SELECTION_PREF_Branch_Company AS SELECTION_PREF_Branch_Company,
    in1.SEGMENT_CODE_Branch_Company AS SEGMENT_CODE_Branch_Company,
    in1.prophecy_sk AS prophecy_sk,
    in1.SEGMENT_CODE_Client_Branch_Company AS SEGMENT_CODE_Client_Branch_Company,
    in1.SELECTION_PREF_Client_Branch_Company AS SELECTION_PREF_Client_Branch_Company,
    in1.SEGMENT_CODE_Client AS SEGMENT_CODE_Client,
    in1.SELECTION_PREF_Client AS SELECTION_PREF_Client
  
  FROM `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_DIM_GI_REFERENCE` AS in0
  LEFT JOIN EXP_Pick_Segment_1_JOIN_EXPR_280 AS in1
     ON (
      (((in0.BUSN_AREA_NAME = 'GI') AND (in0.CMPY_CODE = 'EBI')) AND (in0.ITEM_TABLE = 'EBI0043'))
      AND (in0.ITEM_CODE = UPPER(CAST((LTRIM((RTRIM(in1.SEGMENT_CODE_Client)))) AS string)))
    )
  RIGHT JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_DIM_GI_REFERENCE` AS in2
     ON (
      (((in2.BUSN_AREA_NAME = 'GI') AND (in2.CMPY_CODE = 'EBI')) AND (in2.ITEM_TABLE = 'EBI0043'))
      AND (in2.ITEM_CODE = UPPER(CAST((LTRIM((RTRIM(in1.SEGMENT_CODE_Branch_Company)))) AS string)))
    )
  RIGHT JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_DIM_GI_REFERENCE` AS in3
     ON (
      (((in3.BUSN_AREA_NAME = 'GI') AND (in3.CMPY_CODE = 'EBI')) AND (in3.ITEM_TABLE = 'EBI0043'))
      AND (in3.ITEM_CODE = upper(ltrim(rtrim(in1.SEGMENT_CODE_Client_Branch_Company))))
    )

),

EXP_Pick_Segment_1 AS (

  SELECT 
    (
      CASE
        WHEN ((LEAST(SELECTION_PREF_Client, SELECTION_PREF_Branch_Company, SELECTION_PREF_Client_Branch_Company)) = 10000)
          THEN -1
        WHEN ((LEAST(SELECTION_PREF_Client, SELECTION_PREF_Branch_Company, SELECTION_PREF_Client_Branch_Company)) = SELECTION_PREF_Client)
          THEN LOOKUP_VARIABLE_4
        WHEN ((LEAST(SELECTION_PREF_Client, SELECTION_PREF_Branch_Company, SELECTION_PREF_Client_Branch_Company)) = SELECTION_PREF_Branch_Company)
          THEN LOOKUP_VARIABLE_5
        ELSE LOOKUP_VARIABLE_6
      END
    ) AS SEGMENT_ID,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Pick_Segment_1_LOOKUP_merged AS in0

),

EXP_Collect_All_Data_JOIN_merged AS (

  SELECT 
    in3.TRANS_REC_TYPE AS TRANS_REC_TYPE,
    in3.TRANS_AMT AS TRANS_AMT,
    in6.IN_RSK_ID AS IN_RSK_ID,
    in3.RI_METHOD_CODE AS RI_METHOD_CODE,
    in3.ACCTING_MTH_ID AS ACCTING_MTH_ID,
    in10.DISP_ITEM_DESC AS DISP_ITEM_DESC,
    in9.IN_CLAIM_PAYMENT_TYPE_CODE AS IN_CLAIM_PAYMENT_TYPE_CODE,
    in3.CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    in11.ICA_CAT_CODE AS ICA_CAT_CODE,
    in2.BODILY_INJURY_CODE AS BODILY_INJURY_CODE,
    in1.SUBSEGMENT_ID AS SUBSEGMENT_ID,
    in3.AMT_TYPE_CODE AS AMT_TYPE_CODE,
    in11.UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    in0.SEGMENT_ID AS SEGMENT_ID,
    in3.RSK_CLASS_ID AS RSK_CLASS_ID,
    in10.ITEM_LONG_DESC AS ITEM_LONG_DESC,
    in3.CLAIM_ID AS CLAIM_ID,
    in11.CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    in3.CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    in11.COVR_EXPIRY_DATE AS COVR_EXPIRY_DATE,
    in6.IN_DATE_OF_LOSS AS IN_DATE_OF_LOSS,
    in8.PREM_CLASS_ID AS PREM_CLASS_ID,
    in15.IN_ITEM_CODE AS IN_ITEM_CODE,
    in17.LOSS_DATE_ID AS LOSS_DATE_ID,
    in10.REFERENCE_ID AS REFERENCE_ID,
    in11.CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE,
    in11.CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    in7.PREM_CLASS_CODE AS PREM_CLASS_CODE,
    in7.PREM_COVR_ID AS PREM_COVR_ID,
    in0.prophecy_sk AS prophecy_sk,
    in10.ITEM_SHORT_DESC AS ITEM_SHORT_DESC,
    in17.LOSS_MTH_ID AS LOSS_MTH_ID,
    in14.PRODUCT_ID AS PRODUCT_ID,
    in3.TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    in7.INCEPTION_DATE AS INCEPTION_DATE,
    in3.TRANS_TS AS TRANS_TS,
    in3.BUSN_AREA_NAME AS BUSN_AREA_NAME,
    in9.IN_BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    in11.COVR_INCEPTION_DATE AS COVR_INCEPTION_DATE,
    in17.RPTED_MTH_ID AS RPTED_MTH_ID,
    in7.RSK_ID AS RSK_ID,
    in12.PREM_CLASS_CODE_OUT AS PREM_CLASS_CODE_OUT,
    in17.RPTED_DATE_ID AS RPTED_DATE_ID,
    in7.POLICY_NUMBER AS POLICY_NUMBER,
    in10.ITEM_CODE AS ITEM_CODE,
    in3.TRANS_CCY_CODE AS TRANS_CCY_CODE,
    in3.TRANS_BATCH_TYPE AS TRANS_BATCH_TYPE,
    in5.POL_ID AS POL_ID,
    in5.AGENT_PARTY_ID AS AGENT_PARTY_ID,
    in11.CLIENT_PARTY_ID AS CLIENT_PARTY_ID,
    in7.EXPIRY_DATE AS EXPIRY_DATE,
    in4.IN_POL_ID AS IN_POL_ID,
    in3.CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    in3.TRANS_ID AS TRANS_ID
  
  FROM EXP_Pick_Segment_1 AS in0
  INNER JOIN EXP_Default_Ids AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)
  INNER JOIN EXP_Calculate_Bodily_Injury_Code AS in2
     ON (in1.prophecy_sk = in2.prophecy_sk)
  INNER JOIN SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_0 AS in3
     ON (in2.prophecy_sk = in3.prophecy_sk)
  INNER JOIN LKP_Dim_GI_Claim_By_ClaimID_JOIN_EXPR_314 AS in4
     ON (in3.prophecy_sk = in4.prophecy_sk)
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Policy_Agent` AS in5
     ON (in5.POL_ID = CAST(in4.IN_POL_ID AS INTEGER))
  INNER JOIN LKP_Dim_GI_Claim_By_ClaimID_JOIN_EXPR_311 AS in6
     ON in4.prophecy_sk = in6.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Risk_Premium_Cover` AS in7
     ON (
      (
        (in7.RSK_ID = CAST(in6.IN_RSK_ID AS INTEGER))
        AND (in7.INCEPTION_DATE <= CAST(in6.IN_DATE_OF_LOSS AS TIMESTAMP (0)))
      )
      AND (in7.EXPIRY_DATE > CAST(in6.IN_DATE_OF_LOSS AS TIMESTAMP (0)))
    )
  INNER JOIN sh_LKP_V_Dim_GI_Prem_Class_JOIN AS in8
     ON in6.prophecy_sk = in8.prophecy_sk
  INNER JOIN SQ_sh_DATA_GI_CLAIM_TRANSACTION_GENERATE_SK_EXPR_310 AS in9
     ON (in8.prophecy_sk = in9.prophecy_sk)
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Claim_Payment_Type` AS in10
     ON ((in10.BUSN_AREA_NAME = in9.IN_BUSN_AREA_NAME) AND (in10.ITEM_CODE = in9.IN_CLAIM_PAYMENT_TYPE_CODE))
  INNER JOIN LKP_Dim_GI_Claim_By_ClaimID_JOIN AS in11
     ON in9.prophecy_sk = in11.prophecy_sk
  INNER JOIN EXP_Get_Default_Prem_Class_If_Not_Present AS in12
     ON (in11.prophecy_sk = in12.prophecy_sk)
  INNER JOIN sh_LKP_Dim_GI_Branch_JOIN AS in13
     ON (in12.prophecy_sk = in13.prophecy_sk)
  INNER JOIN sh_LKP_V_Dim_GI_Product_JOIN AS in14
     ON (in13.prophecy_sk = in14.prophecy_sk)
  INNER JOIN sh_LKP_Dim_GI_Accessor_PRE_JOIN_JOIN_EXPR_284 AS in15
     ON (in14.prophecy_sk = in15.prophecy_sk)
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_sh_LKP_Dim_GI_Accessor` AS in16
     ON ((in16.BUSN_AREA_NAME = in15.IN_BUSN_AREA_NAME) AND (in16.ITEM_CODE = in15.IN_ITEM_CODE))
  INNER JOIN EXP_Get_DATE_Id_and_MTH_Id AS in17
     ON in15.prophecy_sk = in17.prophecy_sk

),

EXP_Collect_All_Data AS (

  SELECT 
    (
      CASE
        WHEN CAST((REFERENCE_ID IS NULL) AS BOOLEAN)
          THEN CAST(-1 AS string)
        ELSE REFERENCE_ID
      END
    ) AS CLAIM_PAYMT_TYPE_ID_Out,
    TRANS_ID AS TRANS_ID,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    CURRENT_TIMESTAMP AS CTL_REC_CRTN_DATE,
    {{ var('PMWorkflowRunId') }} AS CTL_JOB_ID,
    CLAIM_ID AS CLAIM_ID,
    ACCTING_MTH_ID AS ACCTING_MTH_ID,
    AMT_TYPE_CODE AS AMT_TYPE_CODE,
    RSK_CLASS_ID AS RSK_CLASS_ID,
    RI_METHOD_CODE AS RI_METHOD_CODE,
    TRANS_REC_TYPE AS TRANS_REC_TYPE,
    TRANS_TS AS TRANS_TS,
    TRANS_AMT AS TRANS_AMT,
    TRANS_CCY_CODE AS TRANS_CCY_CODE,
    TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    TRANS_BATCH_TYPE AS TRANS_BATCH_TYPE,
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    REFERENCE_ID AS ASSESSOR_ID,
    RPTED_DATE_ID AS RPTED_DATE_ID,
    RPTED_MTH_ID AS RPTED_MTH_ID,
    LOSS_DATE_ID AS LOSS_DATE_ID,
    LOSS_MTH_ID AS LOSS_MTH_ID,
    ICA_CAT_CODE AS ICA_CAT_CODE,
    CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    POL_ID AS POLICY_ID,
    UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    CLIENT_PARTY_ID AS CLIENT_ID,
    CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE,
    SEGMENT_ID AS MARKET_SEGMENT_ID,
    PREM_CLASS_CODE_OUT AS PREM_CLASS_CODE,
    CAST((
      TO_TIMESTAMP(
        (
          CASE
            WHEN (BUSN_AREA_NAME = 'NZA')
              THEN EXPIRY_DATE
            ELSE CAST(COVR_EXPIRY_DATE AS string)
          END
        ), 
        'yyyyMMdd')
    ) AS INTEGER) AS EXPIRY_DATE_ID,
    CAST((
      TO_TIMESTAMP(
        (
          CASE
            WHEN (BUSN_AREA_NAME = 'NZA')
              THEN INCEPTION_DATE
            ELSE CAST(COVR_INCEPTION_DATE AS string)
          END
        ), 
        'yyyyMMdd')
    ) AS INTEGER) AS INCEPTION_DATE_ID,
    SEGMENT_ID AS CLAIM_SEGMENT_ID,
    SUBSEGMENT_ID AS CLAIM_SUBSEGMENT_ID,
    BODILY_INJURY_CODE AS BODILY_INJURY_CODE,
    REFERENCE_ID AS POL_BRANCH_ID,
    PREM_CLASS_ID AS PREM_CLASS_ID,
    PRODUCT_ID AS PRD_ID,
    AGENT_PARTY_ID AS INTERMEDIARY_ID,
    prophecy_sk AS prophecy_sk,
    TRANS_ID AS TRANSACTION_ID,
    TRANS_BATCH_TYPE AS BATCH_TRANS_TYPE,
    TRANS_TYPE_CODE AS MTRN_TYPE_CODE,
    AMT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    TRANS_AMT AS AMOUNT_TYPE_VALUE
  
  FROM EXP_Collect_All_Data_JOIN_merged AS in0

),

EXP_Collect_All_Data_EXPR_316 AS (

  SELECT 
    CTL_JOB_ID AS CTL_JOB_ID,
    POL_BRANCH_ID AS POL_BRANCH_ID,
    INTERMEDIARY_ID AS INTERMEDIARY_ID,
    TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    BODILY_INJURY_CODE AS BODILY_INJURY_CODE,
    CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    ICA_CAT_CODE AS ICA_CAT_CODE,
    TRANS_CCY_CODE AS TRANS_CCY_CODE,
    TRANS_TS AS TRANS_TS,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    CLAIM_PAYMT_TYPE_ID_Out AS CLAIM_PAYMT_TYPE_ID,
    ASSESSOR_ID AS ASSESSOR_ID,
    UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE,
    CLIENT_ID AS CLIENT_ID,
    MARKET_SEGMENT_ID AS MARKET_SEGMENT_ID,
    LOSS_DATE_ID AS LOSS_DATE_ID,
    INCEPTION_DATE_ID AS INCEPTION_DATE_ID,
    TRANS_ID AS TRANS_ID,
    POLICY_ID AS POLICY_ID,
    PRD_ID AS PRD_ID,
    RSK_CLASS_ID AS RSK_CLASS_ID,
    CLAIM_SEGMENT_ID AS CLAIM_SEGMENT_ID,
    RPTED_MTH_ID AS RPTED_MTH_ID,
    EXPIRY_DATE_ID AS EXPIRY_DATE_ID,
    ACCTING_MTH_ID AS ACCTING_MTH_ID,
    CLAIM_ID AS CLAIM_ID,
    PREM_CLASS_ID AS PREM_CLASS_ID,
    RI_METHOD_CODE AS RI_METHOD_CODE,
    CLAIM_SUBSEGMENT_ID AS CLAIM_SUBSEGMENT_ID,
    RPTED_DATE_ID AS RPTED_DATE_ID,
    LOSS_MTH_ID AS LOSS_MTH_ID,
    prophecy_sk AS prophecy_sk,
    AMT_TYPE_CODE AS AMT_TYPE_CODE,
    TRANS_REC_TYPE AS TRANS_REC_TYPE,
    TRANS_AMT AS TRANS_AMT,
    TRANS_BATCH_TYPE AS TRANS_BATCH_TYPE,
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM EXP_Collect_All_Data AS in0

),

EXP_Trim_Input AS (

  SELECT 
    TRANSACTION_ID AS TRANSACTION_ID,
    (LTRIM((RTRIM(BUSN_AREA_NAME)))) AS OUT_BUSN_AREA_NAME,
    (LTRIM((RTRIM(TRANS_REC_TYPE)))) AS OUT_TRANS_REC_TYPE,
    (LTRIM((RTRIM(BATCH_TRANS_TYPE)))) AS OUT_BATCH_TRANS_TYPE,
    (LTRIM((RTRIM(MTRN_TYPE_CODE)))) AS OUT_MTRN_TYPE_CODE,
    (LTRIM((RTRIM(PREM_CLASS_CODE)))) AS OUT_PREM_CLASS_CODE,
    (LTRIM((RTRIM(AMOUNT_TYPE_CODE)))) AS OUT_AMOUNT_TYPE_CODE,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    'APC' AS TO_GL_AMT_TYP_APC,
    'GOL' AS TO_GL_AMT_TYP_GOL,
    'GPC' AS TO_GL_AMT_TYP_GPC,
    'RPC' AS TO_GL_AMT_TYP_RPC,
    'ROL' AS TO_GL_AMT_TYP_ROL,
    'AOL' AS TO_GL_AMT_TYP_AOL,
    'SPC' AS TO_GL_AMT_TYP_SPC,
    'SOL' AS TO_GL_AMT_TYP_SOL,
    'TPC' AS TO_GL_AMT_TYP_TPC,
    'TOL' AS TO_GL_AMT_TYP_TOL,
    'UPC' AS TO_GL_AMT_TYP_UPC,
    'UOL' AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Collect_All_Data AS in0

),

EXP_Trim_Input_EXPR_319 AS (

  SELECT 
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYPE,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_320 AS (

  SELECT 
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_317 AS (

  SELECT 
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_324 AS (

  SELECT 
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TRANSACTION_ID AS TRANSACTION_ID,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYPE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_330 AS (

  SELECT 
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYPE,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_321 AS (

  SELECT 
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYPE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_332 AS (

  SELECT 
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYPE,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_327 AS (

  SELECT 
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYPE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_323 AS (

  SELECT 
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYPE,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_331 AS (

  SELECT 
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYPE,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_322 AS (

  SELECT 
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYPE,
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYP_APC,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

EXP_Trim_Input_EXPR_318 AS (

  SELECT 
    OUT_BUSN_AREA_NAME AS BUSN_AREA_NAME,
    OUT_MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    OUT_PREM_CLASS_CODE AS PREM_CLASS_CODE,
    TO_GL_AMT_TYP_APC AS TO_GL_AMT_TYPE,
    OUT_TRANS_REC_TYPE AS TRANS_REC_TYPE,
    OUT_BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    OUT_AMOUNT_TYPE_CODE AS AMOUNT_TYPE_CODE,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    TRANSACTION_ID AS TRANSACTION_ID,
    TO_GL_AMT_TYP_GOL AS TO_GL_AMT_TYP_GOL,
    TO_GL_AMT_TYP_GPC AS TO_GL_AMT_TYP_GPC,
    TO_GL_AMT_TYP_RPC AS TO_GL_AMT_TYP_RPC,
    TO_GL_AMT_TYP_ROL AS TO_GL_AMT_TYP_ROL,
    TO_GL_AMT_TYP_AOL AS TO_GL_AMT_TYP_AOL,
    TO_GL_AMT_TYP_SPC AS TO_GL_AMT_TYP_SPC,
    TO_GL_AMT_TYP_SOL AS TO_GL_AMT_TYP_SOL,
    TO_GL_AMT_TYP_TPC AS TO_GL_AMT_TYP_TPC,
    TO_GL_AMT_TYP_TOL AS TO_GL_AMT_TYP_TOL,
    TO_GL_AMT_TYP_UPC AS TO_GL_AMT_TYP_UPC,
    TO_GL_AMT_TYP_UOL AS TO_GL_AMT_TYP_UOL
  
  FROM EXP_Trim_Input AS in0

),

UN_Create_All_GL_Amt_Type_For_Movement AS (

  {{
    prophecy_basics.UnionByName(
      [
        'EXP_Trim_Input_EXPR_318', 
        'EXP_Trim_Input_EXPR_322', 
        'EXP_Trim_Input_EXPR_330', 
        'EXP_Trim_Input_EXPR_327', 
        'EXP_Trim_Input_EXPR_323', 
        'EXP_Trim_Input_EXPR_331', 
        'EXP_Trim_Input_EXPR_332', 
        'EXP_Trim_Input_EXPR_317', 
        'EXP_Trim_Input_EXPR_320', 
        'EXP_Trim_Input_EXPR_324', 
        'EXP_Trim_Input_EXPR_321', 
        'EXP_Trim_Input_EXPR_319'
      ], 
      [
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]', 
        '[{"name": "BATCH_TRANS_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_VALUE", "dataType": "Double"}, {"name": "TRANSACTION_ID", "dataType": "Integer"}, {"name": "BUSN_AREA_NAME", "dataType": "String"}, {"name": "PREM_CLASS_CODE", "dataType": "String"}, {"name": "TO_GL_AMT_TYPE", "dataType": "String"}, {"name": "AMOUNT_TYPE_CODE", "dataType": "String"}, {"name": "MTRN_TYPE_CODE", "dataType": "String"}, {"name": "TRANS_REC_TYPE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_sk,
    *
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement AS in0

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_MTRN_Typ_and_Prem_Class` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_MTRN_Typ_and_Prem_Class'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_Prem_Class_Irrlvnt_MTRN_Typ` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_Prem_Class_Irrlvnt_MTRN_Typ'
    )
  }}

),

`35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Exclude_Batch_Trans_Typ` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Data_GI_Claim_Movement', 
      '35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Exclude_Batch_Trans_Typ'
    )
  }}

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_294 AS (

  SELECT 
    TRANS_REC_TYPE AS IN_TRANS_REC_TYPE,
    MTRN_TYPE_CODE AS IN_MTRN_TYPE_CODE,
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    TO_GL_AMT_TYPE AS IN_TO_GL_AMT_TYPE,
    AMOUNT_TYPE_CODE AS IN_AMOUNT_TYPE_CODE,
    prophecy_sk AS prophecy_sk,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in0

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_291 AS (

  SELECT 
    MTRN_TYPE_CODE AS IN_MTRN_TYPE_CODE,
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    TRANS_REC_TYPE AS IN_TRANS_REC_TYPE,
    AMOUNT_TYPE_CODE AS IN_AMOUNT_TYPE_CODE,
    TO_GL_AMT_TYPE AS IN_TO_GL_AMT_TYPE,
    prophecy_sk AS prophecy_sk,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in0

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_295 AS (

  SELECT 
    TRANS_REC_TYPE AS IN_TRANS_REC_TYPE,
    AMOUNT_TYPE_CODE AS IN_AMOUNT_TYPE_CODE,
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    TO_GL_AMT_TYPE AS IN_TO_GL_AMT_TYPE,
    prophecy_sk AS prophecy_sk,
    MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in0

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_296 AS (

  SELECT 
    TO_GL_AMT_TYPE AS in_TO_GL_AMT_TYPE,
    PREM_CLASS_CODE AS in_PREM_CLASS_CODE,
    AMOUNT_TYPE_CODE AS in_AMOUNT_TYPE_CODE,
    BUSN_AREA_NAME AS in_BUSN_AREA_NAME,
    TRANS_REC_TYPE AS in_TRANS_REC_TYPE,
    prophecy_sk AS prophecy_sk,
    MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in0

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_292 AS (

  SELECT 
    TO_GL_AMT_TYPE AS IN_TO_GL_AMT_TYPE,
    AMOUNT_TYPE_CODE AS IN_AMOUNT_TYPE_CODE,
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    TRANS_REC_TYPE AS IN_TRANS_REC_TYPE,
    prophecy_sk AS prophecy_sk,
    MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in0

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_297 AS (

  SELECT 
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    TO_GL_AMT_TYPE AS IN_TO_GL_AMT_TYPE,
    PREM_CLASS_CODE AS IN_PREM_CLASS_CODE,
    AMOUNT_TYPE_CODE AS IN_AMOUNT_TYPE_CODE,
    TRANS_REC_TYPE AS IN_TRANS_REC_TYPE,
    MTRN_TYPE_CODE AS IN_MTRN_TYPE_CODE,
    prophecy_sk AS prophecy_sk,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    BATCH_TRANS_TYPE AS BATCH_TRANS_TYPE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in0

),

UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_293 AS (

  SELECT 
    BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    BATCH_TRANS_TYPE AS IN_BATCH_TRANS_TYPE,
    AMOUNT_TYPE_CODE AS IN_AMOUNT_TYPE_CODE,
    TO_GL_AMT_TYPE AS IN_TO_GL_AMT_TYPE,
    TRANS_REC_TYPE AS IN_TRANS_REC_TYPE,
    prophecy_sk AS prophecy_sk,
    MTRN_TYPE_CODE AS MTRN_TYPE_CODE,
    TRANSACTION_ID AS TRANSACTION_ID,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    PREM_CLASS_CODE AS PREM_CLASS_CODE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in0

),

EXP_Calculate_Converted_Amt_Type_JOIN_merged AS (

  SELECT 
    in1.TRANS_REC_TYPE AS TRANS_REC_TYPE,
    in0.IN_TO_GL_AMT_TYPE AS IN_TO_GL_AMT_TYPE,
    in2.IN_MTRN_TYPE_CODE AS IN_MTRN_TYPE_CODE,
    in0.IN_AMOUNT_TYPE_CODE AS IN_AMOUNT_TYPE_CODE,
    in1.FROM_BUSN_AREA_NAME AS FROM_BUSN_AREA_NAME,
    in0.IN_TRANS_REC_TYPE AS IN_TRANS_REC_TYPE,
    in1.EXCLUDE_BATCH_TRANS_TYPE AS EXCLUDE_BATCH_TRANS_TYPE,
    in4.AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    in1.CALCULATION_FACTOR AS CALCULATION_FACTOR,
    in4.TRANSACTION_ID AS TRANSACTION_ID,
    in1.EXCLUDE_MTRN_TYPE AS EXCLUDE_MTRN_TYPE,
    in0.prophecy_sk AS prophecy_sk,
    in1.FROM_MTRN_TYPE AS FROM_MTRN_TYPE,
    in7.IN_PREM_CLASS_CODE AS IN_PREM_CLASS_CODE,
    in0.IN_BUSN_AREA_NAME AS IN_BUSN_AREA_NAME,
    in1.FROM_PREM_CLASS AS FROM_PREM_CLASS,
    in1.TO_AMT_TYPE_DESC AS TO_AMT_TYPE_DESC,
    in1.TO_GL_AMT_TYPE AS TO_GL_AMT_TYPE,
    in1.FROM_AMT_TYPE_CODE AS FROM_AMT_TYPE_CODE,
    in0.IN_BATCH_TRANS_TYPE AS IN_BATCH_TRANS_TYPE
  
  FROM UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_293 AS in0
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Exclude_Batch_Trans_Typ` AS in1
     ON (
      (
        (
          ((in1.FROM_BUSN_AREA_NAME = in0.IN_BUSN_AREA_NAME) AND (in1.TO_GL_AMT_TYPE = in0.IN_TO_GL_AMT_TYPE))
          AND (in1.TRANS_REC_TYPE = in0.IN_TRANS_REC_TYPE)
        )
        AND (in1.FROM_AMT_TYPE_CODE = in0.IN_AMOUNT_TYPE_CODE)
      )
      AND (in1.EXCLUDE_BATCH_TRANS_TYPE = in0.IN_BATCH_TRANS_TYPE)
    )
  INNER JOIN UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_291 AS in2
     ON in0.prophecy_sk = in2.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_MTRN_Typ_Irrlvnt_Prem_Class` AS in3
     ON (
      (
        (
          ((in3.FROM_BUSN_AREA_NAME = in2.IN_BUSN_AREA_NAME) AND (in3.TO_GL_AMT_TYPE = in2.IN_TO_GL_AMT_TYPE))
          AND (in3.TRANS_REC_TYPE = in2.IN_TRANS_REC_TYPE)
        )
        AND (in3.FROM_AMT_TYPE_CODE = in2.IN_AMOUNT_TYPE_CODE)
      )
      AND (in3.FROM_MTRN_TYPE = in2.IN_MTRN_TYPE_CODE)
    )
  INNER JOIN UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_0 AS in4
     ON in2.prophecy_sk = in4.prophecy_sk
  INNER JOIN UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_294 AS in5
     ON (in4.prophecy_sk = in5.prophecy_sk)
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Exclude_MTRN_Type` AS in6
     ON (
      (
        (
          ((in6.FROM_BUSN_AREA_NAME = in5.IN_BUSN_AREA_NAME) AND (in6.TO_GL_AMT_TYPE = in5.IN_TO_GL_AMT_TYPE))
          AND (in6.TRANS_REC_TYPE = in5.IN_TRANS_REC_TYPE)
        )
        AND (in6.FROM_AMT_TYPE_CODE = in5.IN_AMOUNT_TYPE_CODE)
      )
      AND (in6.EXCLUDE_MTRN_TYPE = in5.IN_MTRN_TYPE_CODE)
    )
  INNER JOIN UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_297 AS in7
     ON in5.prophecy_sk = in7.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_MTRN_Typ_and_Prem_Class` AS in8
     ON (
      (
        (
          (
            ((in8.FROM_BUSN_AREA_NAME = in7.IN_BUSN_AREA_NAME) AND (in8.TO_GL_AMT_TYPE = in7.IN_TO_GL_AMT_TYPE))
            AND (in8.TRANS_REC_TYPE = in7.IN_TRANS_REC_TYPE)
          )
          AND (in8.FROM_AMT_TYPE_CODE = in7.IN_AMOUNT_TYPE_CODE)
        )
        AND (in8.FROM_MTRN_TYPE = in7.IN_MTRN_TYPE_CODE)
      )
      AND (in8.FROM_PREM_CLASS = in7.IN_PREM_CLASS_CODE)
    )
  INNER JOIN UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_295 AS in9
     ON in7.prophecy_sk = in9.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Irrlvnt_MTRN_Typ_PREM_Class` AS in10
     ON (
      (
        ((in10.FROM_BUSN_AREA_NAME = in9.IN_BUSN_AREA_NAME) AND (in10.TO_GL_AMT_TYPE = in9.IN_TO_GL_AMT_TYPE))
        AND (in10.TRANS_REC_TYPE = in9.IN_TRANS_REC_TYPE)
      )
      AND (in10.FROM_AMT_TYPE_CODE = in9.IN_AMOUNT_TYPE_CODE)
    )
  INNER JOIN UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_296 AS in11
     ON in9.prophecy_sk = in11.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_Matching_Prem_Class_Irrlvnt_MTRN_Typ` AS in12
     ON (
      (
        (
          (
            (in12.FROM_BUSN_AREA_NAME = in11.in_BUSN_AREA_NAME)
            AND (in12.TO_GL_AMT_TYPE = in11.in_TO_GL_AMT_TYPE)
          )
          AND (in12.TRANS_REC_TYPE = in11.in_TRANS_REC_TYPE)
        )
        AND (in12.FROM_AMT_TYPE_CODE = in11.in_AMOUNT_TYPE_CODE)
      )
      AND (in12.FROM_PREM_CLASS = in11.in_PREM_CLASS_CODE)
    )
  INNER JOIN UN_Create_All_GL_Amt_Type_For_Movement_GENERATE_SK_EXPR_292 AS in13
     ON in11.prophecy_sk = in13.prophecy_sk
  INNER JOIN `35_SDMmp_mdl_sdm_Data_GI_Claim_Movement_SOURCE_LKP_DIM_GI_CLAIM_AMOUNT_TYPE_MAPPING_All_Irrlvnt` AS in14
     ON (
      (
        (
          (in14.FROM_BUSN_AREA_NAME = in13.IN_BUSN_AREA_NAME)
          AND (in14.TO_GL_AMT_TYPE = in13.IN_TO_GL_AMT_TYPE)
        )
        AND (in14.TRANS_REC_TYPE = in13.IN_TRANS_REC_TYPE)
      )
      AND (in14.FROM_AMT_TYPE_CODE = in13.IN_AMOUNT_TYPE_CODE)
    )

),

EXP_Calculate_Converted_Amt_Type AS (

  SELECT 
    TRANSACTION_ID AS TRANSACTION_ID,
    TO_GL_AMT_TYPE AS TO_GL_AMT_TYPE,
    AMOUNT_TYPE_VALUE AS AMOUNT_TYPE_VALUE,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'APC')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS APC_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'AOL')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS AOL_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'GPC')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS GPC_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'GOL')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS GOL_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'RPC')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS RPC_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'ROL')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS ROL_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'SPC')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS SPC_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'SOL')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS SOL_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'TPC')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS TPC_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'TOL')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS TOL_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'UPC')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS UPC_AMT,
    (
      CASE
        WHEN (TO_GL_AMT_TYPE = 'UOL')
          THEN (
            CASE
              WHEN (
                (
                  CASE
                    WHEN ((NOT(FROM_BUSN_AREA_NAME IS NULL)) OR (NOT(FROM_BUSN_AREA_NAME IS NULL)))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN 0
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              WHEN (
                (
                  CASE
                    WHEN (NOT(FROM_BUSN_AREA_NAME IS NULL))
                      THEN 'Y'
                    ELSE 'N'
                  END
                ) = 'Y'
              )
                THEN (AMOUNT_TYPE_VALUE * CALCULATION_FACTOR)
              ELSE NULL
            END
          )
        ELSE NULL
      END
    ) AS UOL_AMT,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Calculate_Converted_Amt_Type_JOIN_merged AS in0

),

AGG_To_Get_All_Gl_Amt_Type_In_One_Record AS (

  SELECT 
    first(APC_AMT) AS APC_AMT_OUT,
    first(AOL_AMT) AS AOL_AMT_OUT,
    first(GPC_AMT) AS GPC_AMT_OUT,
    first(GOL_AMT) AS GOL_AMT_OUT,
    first(RPC_AMT) AS RPC_AMT_OUT,
    first(ROL_AMT) AS ROL_AMT_OUT,
    first(SPC_AMT) AS SPC_AMT_OUT,
    first(SOL_AMT) AS SOL_AMT_OUT,
    first(TPC_AMT) AS TPC_AMT_OUT,
    first(TOL_AMT) AS TOL_AMT_OUT,
    first(UPC_AMT) AS UPC_AMT_OUT,
    first(UOL_AMT) AS UOL_AMT_OUT,
    first(TRANSACTION_ID) AS TRANSACTION_ID
  
  FROM EXP_Calculate_Converted_Amt_Type AS in0
  
  GROUP BY TRANSACTION_ID

),

AGG_To_Get_All_Gl_Amt_Type_In_One_Record_EXPR_290 AS (

  SELECT 
    TRANSACTION_ID AS MPPLT_TRANSACTION_ID,
    APC_AMT_OUT AS APC_AMT,
    AOL_AMT_OUT AS AOL_AMT,
    GPC_AMT_OUT AS GPC_AMT,
    GOL_AMT_OUT AS GOL_AMT,
    RPC_AMT_OUT AS RPC_AMT,
    ROL_AMT_OUT AS ROL_AMT,
    SPC_AMT_OUT AS SPC_AMT,
    SOL_AMT_OUT AS SOL_AMT,
    TPC_AMT_OUT AS TPC_AMT,
    TOL_AMT_OUT AS TOL_AMT,
    UPC_AMT_OUT AS UPC_AMT,
    UOL_AMT_OUT AS UOL_AMT
  
  FROM AGG_To_Get_All_Gl_Amt_Type_In_One_Record AS in0

),

JNR_Join_With_Amt_Mapplet AS (

  SELECT 
    in1.CLAIM_SUBSEGMENT_ID AS CLAIM_SUBSEGMENT_ID,
    in1.RI_METHOD_CODE AS RI_METHOD_CODE,
    in0.MPPLT_TRANSACTION_ID AS MPPLT_TRANSACTION_ID,
    in1.ACCTING_MTH_ID AS ACCTING_MTH_ID,
    in1.POLICY_ID AS POLICY_ID,
    in0.GPC_AMT AS GPC_AMT,
    in0.ROL_AMT AS ROL_AMT,
    in1.MARKET_SEGMENT_ID AS MARKET_SEGMENT_ID,
    in1.ASSESSOR_ID AS ASSESSOR_ID,
    in0.UPC_AMT AS UPC_AMT,
    in1.CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    in0.TOL_AMT AS TOL_AMT,
    in0.TPC_AMT AS TPC_AMT,
    in1.ICA_CAT_CODE AS ICA_CAT_CODE,
    in0.UOL_AMT AS UOL_AMT,
    in1.BODILY_INJURY_CODE AS BODILY_INJURY_CODE,
    in1.UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    in1.RSK_CLASS_ID AS RSK_CLASS_ID,
    in1.POL_BRANCH_ID AS POL_BRANCH_ID,
    in1.CLAIM_ID AS CLAIM_ID,
    in1.CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    in1.CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    in1.CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    in0.APC_AMT AS APC_AMT,
    in1.CLIENT_ID AS CLIENT_ID,
    in1.INCEPTION_DATE_ID AS INCEPTION_DATE_ID,
    in1.PREM_CLASS_ID AS PREM_CLASS_ID,
    in1.LOSS_DATE_ID AS LOSS_DATE_ID,
    in1.CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE,
    in1.CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    in1.prophecy_sk AS prophecy_sk,
    in1.LOSS_MTH_ID AS LOSS_MTH_ID,
    in1.TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    in0.AOL_AMT AS AOL_AMT,
    in0.SOL_AMT AS SOL_AMT,
    in1.TRANS_TS AS TRANS_TS,
    in1.INTERMEDIARY_ID AS INTERMEDIARY_ID,
    in0.SPC_AMT AS SPC_AMT,
    in1.RPTED_MTH_ID AS RPTED_MTH_ID,
    in1.CLAIM_SEGMENT_ID AS CLAIM_SEGMENT_ID,
    in1.RPTED_DATE_ID AS RPTED_DATE_ID,
    in0.GOL_AMT AS GOL_AMT,
    in1.CLAIM_PAYMT_TYPE_ID AS CLAIM_PAYMT_TYPE_ID,
    in1.PRD_ID AS PRD_ID,
    in1.TRANS_CCY_CODE AS TRANS_CCY_CODE,
    in0.RPC_AMT AS RPC_AMT,
    in1.CTL_JOB_ID AS CTL_JOB_ID,
    in1.CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    in1.TRANS_ID AS TRANS_ID,
    in1.EXPIRY_DATE_ID AS EXPIRY_DATE_ID
  
  FROM AGG_To_Get_All_Gl_Amt_Type_In_One_Record_EXPR_290 AS in0
  INNER JOIN EXP_Collect_All_Data_EXPR_316 AS in1
     ON (in0.MPPLT_TRANSACTION_ID = in1.TRANS_ID)

),

JNR_Join_With_Amt_Mapplet_GENERATE_SK_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_sk,
    *
  
  FROM JNR_Join_With_Amt_Mapplet AS in0

),

EXP_Find_Trans_With_Zero_Amt_Values AS (

  SELECT 
    (
      CASE
        WHEN (
          (
            (
              (
                (
                  (
                    (
                      (((((APC_AMT = 0) AND (GOL_AMT = 0)) AND (GPC_AMT = 0)) AND (RPC_AMT = 0)) AND (ROL_AMT = 0))
                      AND (AOL_AMT = 0)
                    )
                    AND (SPC_AMT = 0)
                  )
                  AND (SOL_AMT = 0)
                )
                AND (TPC_AMT = 0)
              )
              AND (TOL_AMT = 0)
            )
            AND (UPC_AMT = 0)
          )
          AND (UOL_AMT = 0)
        )
          THEN 'Y'
        ELSE 'N'
      END
    ) AS ALL_AMT_VALUE_ZERO,
    prophecy_sk AS prophecy_sk
  
  FROM JNR_Join_With_Amt_Mapplet_GENERATE_SK_0 AS in0

),

FIL_Non_Zero_Amt_Values_JOIN AS (

  SELECT 
    in0.CLAIM_SUBSEGMENT_ID AS CLAIM_SUBSEGMENT_ID,
    in0.RI_METHOD_CODE AS RI_METHOD_CODE,
    in0.ACCTING_MTH_ID AS ACCTING_MTH_ID,
    in0.POLICY_ID AS POLICY_ID,
    in0.GPC_AMT AS GPC_AMT,
    in0.ROL_AMT AS ROL_AMT,
    in0.MARKET_SEGMENT_ID AS MARKET_SEGMENT_ID,
    in0.ASSESSOR_ID AS ASSESSOR_ID,
    in0.UPC_AMT AS UPC_AMT,
    in0.CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    in0.TOL_AMT AS TOL_AMT,
    in0.TPC_AMT AS TPC_AMT,
    in0.ICA_CAT_CODE AS ICA_CAT_CODE,
    in0.UOL_AMT AS UOL_AMT,
    in0.BODILY_INJURY_CODE AS BODILY_INJURY_CODE,
    in0.UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    in0.RSK_CLASS_ID AS RSK_CLASS_ID,
    in0.POL_BRANCH_ID AS POL_BRANCH_ID,
    in0.CLAIM_ID AS CLAIM_ID,
    in0.CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    in0.CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    in0.CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    in0.APC_AMT AS APC_AMT,
    in0.CLIENT_ID AS CLIENT_ID,
    in0.INCEPTION_DATE_ID AS INCEPTION_DATE_ID,
    in0.PREM_CLASS_ID AS PREM_CLASS_ID,
    in0.LOSS_DATE_ID AS LOSS_DATE_ID,
    in0.CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE,
    in0.CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    in1.ALL_AMT_VALUE_ZERO AS ALL_AMT_VALUE_ZERO,
    in0.prophecy_sk AS prophecy_sk,
    in0.LOSS_MTH_ID AS LOSS_MTH_ID,
    in0.TRANS_TYPE_CODE AS TRANS_TYPE_CODE,
    in0.AOL_AMT AS AOL_AMT,
    in0.SOL_AMT AS SOL_AMT,
    in0.TRANS_TS AS TRANS_TS,
    in0.INTERMEDIARY_ID AS INTERMEDIARY_ID,
    in0.SPC_AMT AS SPC_AMT,
    in0.RPTED_MTH_ID AS RPTED_MTH_ID,
    in0.CLAIM_SEGMENT_ID AS CLAIM_SEGMENT_ID,
    in0.RPTED_DATE_ID AS RPTED_DATE_ID,
    in0.GOL_AMT AS GOL_AMT,
    in0.CLAIM_PAYMT_TYPE_ID AS CLAIM_PAYMT_TYPE_ID,
    in0.PRD_ID AS PRD_ID,
    in0.TRANS_CCY_CODE AS TRANS_CCY_CODE,
    in0.RPC_AMT AS RPC_AMT,
    in0.CTL_JOB_ID AS CTL_JOB_ID,
    in0.CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    in0.TRANS_ID AS TRANS_ID,
    in0.EXPIRY_DATE_ID AS EXPIRY_DATE_ID
  
  FROM JNR_Join_With_Amt_Mapplet_GENERATE_SK_0 AS in0
  INNER JOIN EXP_Find_Trans_With_Zero_Amt_Values AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

FIL_Non_Zero_Amt_Values AS (

  SELECT * 
  
  FROM FIL_Non_Zero_Amt_Values_JOIN AS in0
  
  WHERE CAST(TRUE AS BOOLEAN)

),

FIL_Non_Zero_Amt_Values_EXPR_309 AS (

  SELECT 
    CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    ROL_AMT AS ROL_AMT,
    AOL_AMT AS AOL_AMT,
    TPC_AMT AS TPC_AMT,
    MARKET_SEGMENT_ID AS MARKET_SEGMENT_ID,
    RPTED_DATE_ID AS RPTED_DATE_ID,
    LOSS_MTH_ID AS ACDNT_MTH_ID,
    CLAIM_OFFICER_PARTY_NUM AS CLAIM_OFFICER_PARTY_NUM,
    CLAIM_OWNR_CODE AS CLAIM_OWNR_CODE,
    TOL_AMT AS TOL_AMT,
    UPC_AMT AS UPC_AMT,
    CLIENT_ID AS CLIENT_ID,
    UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    ICA_CAT_CODE AS CLAIM_CATASTROPHE_CODE,
    GOL_AMT AS GOL_AMT,
    INTERMEDIARY_ID AS INTERMEDIARY_ID,
    CLAIM_SUBSEGMENT_ID AS CLAIM_SUBSEG_ID,
    LOSS_DATE_ID AS LOSS_DATE_ID,
    BODILY_INJURY_CODE AS BODILY_INJURY_CODE,
    TRANS_ID AS TRANS_ID,
    ACCTING_MTH_ID AS ACCTING_MTH_ID,
    POL_BRANCH_ID AS POL_BRANCH_ID,
    CLAIM_PAYMT_TYPE_ID AS CLAIM_PAYMT_TYPE_ID,
    PREM_CLASS_ID AS PREM_CLASS_ID,
    TRANS_TYPE_CODE AS MTRN_TYPE_CODE,
    INCEPTION_DATE_ID AS INCEPTION_DATE_ID,
    EXPIRY_DATE_ID AS EXPIRY_DATE_ID,
    UOL_AMT AS UOL_AMT,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    TRANS_TS AS TRANS_TS,
    APC_AMT AS APC_AMT,
    SPC_AMT AS SPC_AMT,
    SOL_AMT AS SOL_AMT,
    CLAIM_SEGMENT_ID AS CLAIM_SEGMENT_ID,
    RPTED_MTH_ID AS RPTED_MTH_ID,
    CLAIM_OFFICER_PARTY_ID AS CLAIM_OFFICER_PARTY_ID,
    ASSESSOR_ID AS ASSESSOR_ID,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    CLAIM_ID AS CLAIM_ID,
    POLICY_ID AS POLICY_ID,
    RI_METHOD_CODE AS RI_METHOD_CODE,
    RSK_CLASS_ID AS RSK_CLASS_ID,
    PRD_ID AS PRD_ID,
    TRANS_CCY_CODE AS TRANS_CCY_CODE,
    GPC_AMT AS GPC_AMT,
    RPC_AMT AS RPC_AMT,
    CTL_JOB_ID AS CTL_JOB_ID
  
  FROM FIL_Non_Zero_Amt_Values AS in0

)

SELECT *

FROM FIL_Non_Zero_Amt_Values_EXPR_309
