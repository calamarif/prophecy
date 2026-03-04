{{
  config({    
    "materialized": "table",
    "alias": "Fact_Claim_Tran_Temp",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_gst_tolerance` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_gst_tolerance'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_CHEQ_REQN_TYPE` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_CHEQ_REQN_TYPE'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_PAYMENT_TYPE` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_PAYMENT_TYPE'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_CLAIM` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_CLAIM'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_H_PARTY` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_H_PARTY'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_L_CLAIM_PAYEE_TRAN` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_L_CLAIM_PAYEE_TRAN'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_rt` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_rt'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_payment_type_group` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_payment_type_group'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_ref_payment_type_group` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_ref_payment_type_group'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SQ_S_CLAIM_TRAN` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SQ_S_CLAIM_TRAN'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_company` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_company'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_tt` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_tt'
    )
  }}

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_gst_tolerance_2` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_gst_tolerance_2'
    )
  }}

),

EXPTRANS_LOOKUP_341_LOOKUP_342_LOOKUP_merged AS (

  SELECT 
    in0.GST_TOLERANCE_LOW_AMOUNT AS LOOKUP_VARIABLE_3,
    in2.REF_HK AS LOOKUP_VARIABLE_8,
    in3.REF_HK AS LOOKUP_VARIABLE_11,
    in4.REF_HK AS LOOKUP_VARIABLE_10,
    in5.Country_Code AS LOOKUP_VARIABLE_5,
    in6.REF_HK AS LOOKUP_VARIABLE_9,
    in7.GST_TOLERANCE_LOW_AMOUNT AS LOOKUP_VARIABLE_4,
    in7.GST_TOLERANCE_LOW_AMOUNT AS GST_TOLERANCE_LOW_AMOUNT,
    in7.GST_TOLERANCE_HIGH_AMOUNT AS GST_TOLERANCE_HIGH_AMOUNT,
    in6.REF_HK AS REF_HK,
    in6.REF_BK AS REF_BK,
    in6.CLAIM_TRAN_TYPE_CODE AS CLAIM_TRAN_TYPE_CODE,
    in6.CLAIM_TRAN_TYPE_DESCRIPTION AS CLAIM_TRAN_TYPE_DESCRIPTION,
    in6.CLAIM_TRAN_RECORD_CODE AS CLAIM_TRAN_RECORD_CODE,
    in6.CLAIM_RI_TYPE_CODE AS CLAIM_RI_TYPE_CODE,
    in6.CLAIM_TRAN_MODIFY_TYPE_CODE AS CLAIM_TRAN_MODIFY_TYPE_CODE,
    in6.TRAN_TYPE_CODE AS TRAN_TYPE_CODE,
    in5.Company_Code AS Company_Code,
    in5.Company_Description AS Company_Description,
    in5.Country_Code AS Country_Code,
    in5.Country_GST_Rate AS Country_GST_Rate,
    in5.Country_GST_From_Date_Key AS Country_GST_From_Date_Key,
    in5.Country_GST_To_Date_Key AS Country_GST_To_Date_Key,
    in1.Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    in1.Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    in1.Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    in1.Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    in1.Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    in1.GST_Division_Code AS GST_Division_Code,
    in1.Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    in1.CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    in1.Claim_Payment_Description AS Claim_Payment_Description,
    in1.Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    in1.Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    in1.Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    in1.Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    in1.Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    in1.Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    in1.Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    in1.Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    in1.Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    in1.Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    in1.Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    in1.Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    in1.Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    in1.Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    in1.Claim_Payment_Amount AS Claim_Payment_Amount,
    in1.Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    in1.Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    in1.CTL_BATCH_ID AS CTL_BATCH_ID,
    in1.CTL_JOB_ID AS CTL_JOB_ID,
    in1.CTL_LOAD_DM AS CTL_LOAD_DM,
    in1.TRAN_HK AS TRAN_HK,
    in1.TRAN_BK AS TRAN_BK,
    in1.Claim_Tran_Id AS Claim_Tran_Id,
    in1.CLAIM_HK AS CLAIM_HK,
    in1.CLAIM_BK AS CLAIM_BK,
    in1.Claim_Number AS Claim_Number,
    in1.Payment_Type_Code AS Payment_Type_Code,
    in1.Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    in1.Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    in1.Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    in1.Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    in1.Acct_Period_Code AS Acct_Period_Code,
    in1.Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    in1.Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    in1.Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount
  
  FROM `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_gst_tolerance` AS in0
  LEFT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SQ_S_CLAIM_TRAN` AS in1
     ON (
      (in0.GST_TOLERANCE_LOW_AMOUNT < ABS(in1.Claim_Payment_GST_Difference_Amount))
      AND (in0.GST_TOLERANCE_HIGH_AMOUNT >= ABS(in1.Claim_Payment_GST_Difference_Amount))
    )
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_tt` AS in2
     ON (
      (
        (in2.CLAIM_TRAN_RECORD_CODE = in1.Claim_Tran_Record_Code)
        AND (in2.CLAIM_TRAN_MODIFY_TYPE_CODE = in1.Claim_Tran_Modify_Type_Code)
      )
      AND (in2.CLAIM_RI_TYPE_CODE = in1.Claim_RI_Type_Code)
    )
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_tt` AS in3
     ON (
      (
        (in3.CLAIM_TRAN_RECORD_CODE = in1.Claim_Tran_Record_Code)
        AND (in3.CLAIM_TRAN_MODIFY_TYPE_CODE = '~')
      )
      AND (in3.CLAIM_RI_TYPE_CODE = '~')
    )
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_tt` AS in4
     ON (
      (
        (in4.CLAIM_TRAN_RECORD_CODE = in1.Claim_Tran_Record_Code)
        AND (in4.CLAIM_TRAN_MODIFY_TYPE_CODE = '~')
      )
      AND (in4.CLAIM_RI_TYPE_CODE = in1.Claim_RI_Type_Code)
    )
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_company` AS in5
     ON (in5.Company_Code = substring(in1.CLAIM_BK, 2, 1))
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_tt` AS in6
     ON (
      (
        (in6.CLAIM_TRAN_RECORD_CODE = in1.Claim_Tran_Record_Code)
        AND (in6.CLAIM_TRAN_MODIFY_TYPE_CODE = in1.Claim_Tran_Modify_Type_Code)
      )
      AND (in6.CLAIM_RI_TYPE_CODE = '~')
    )
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_gst_tolerance_2` AS in7
     ON (in7.GST_TOLERANCE_LOW_AMOUNT < abs(in1.Claim_Payment_GST_Difference_Amount))

),

EXPTRANS_LOOKUP_341_LOOKUP_342 AS (

  SELECT 
    in0.PAYMENT_TYPE_GROUP_NAME AS LOOKUP_VARIABLE_6,
    in0.REF_HK AS REF_HK,
    in0.REF_BK AS REF_BK,
    in0.COUNTRY_CODE AS COUNTRY_CODE,
    in0.PAYMENT_TYPE_CODE AS PAYMENT_TYPE_CODE,
    in0.PAYMENT_TYPE_DESCRIPTION AS PAYMENT_TYPE_DESCRIPTION,
    in0.PAYMENT_TYPE_GROUP_NAME AS PAYMENT_TYPE_GROUP_NAME,
    in0.CTL_BATCH_ID AS CTL_BATCH_ID,
    in0.CTL_JOB_ID AS CTL_JOB_ID,
    in0.CTL_SRC_CD AS CTL_SRC_CD,
    in0.CTL_REC_MD5_HA AS CTL_REC_MD5_HA,
    in0.CTL_LOAD_DM AS CTL_LOAD_DM,
    in0.CTL_LOAD_END_DM AS CTL_LOAD_END_DM,
    in0.CTL_DEL_YN AS CTL_DEL_YN,
    in1.Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    in1.Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    in1.Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    in1.Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    in1.Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    in1.GST_Division_Code AS GST_Division_Code,
    in1.Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    in1.CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    in1.Claim_Payment_Description AS Claim_Payment_Description,
    in1.Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    in1.Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    in1.Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    in1.Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    in1.Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    in1.Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    in1.Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    in1.Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    in1.Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    in1.Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    in1.Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    in1.Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    in1.Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    in1.Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    in1.Claim_Payment_Amount AS Claim_Payment_Amount,
    in1.Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    in1.Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    in1.TRAN_HK AS TRAN_HK,
    in1.TRAN_BK AS TRAN_BK,
    in1.Claim_Tran_Id AS Claim_Tran_Id,
    in1.CLAIM_HK AS CLAIM_HK,
    in1.CLAIM_BK AS CLAIM_BK,
    in1.Company_Code AS Company_Code,
    in1.Claim_Number AS Claim_Number,
    in1.Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    in1.Tran_Type_Code AS Tran_Type_Code,
    in1.Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    in1.Claim_RI_Type_Code AS Claim_RI_Type_Code,
    in1.Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    in1.Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    in1.Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    in1.Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    in1.Acct_Period_Code AS Acct_Period_Code,
    in1.Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    in1.Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    in1.Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    in1.LOOKUP_VARIABLE_3 AS LOOKUP_VARIABLE_3,
    in1.LOOKUP_VARIABLE_8 AS LOOKUP_VARIABLE_8,
    in1.LOOKUP_VARIABLE_11 AS LOOKUP_VARIABLE_11,
    in1.LOOKUP_VARIABLE_10 AS LOOKUP_VARIABLE_10,
    in1.LOOKUP_VARIABLE_5 AS LOOKUP_VARIABLE_5,
    in1.LOOKUP_VARIABLE_9 AS LOOKUP_VARIABLE_9,
    in1.LOOKUP_VARIABLE_4 AS LOOKUP_VARIABLE_4
  
  FROM `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_ref_payment_type_group` AS in0
  LEFT JOIN EXPTRANS_LOOKUP_341_LOOKUP_342_LOOKUP_merged AS in1
     ON ((in0.COUNTRY_CODE = in1.LOOKUP_VARIABLE_5) AND (in0.PAYMENT_TYPE_CODE = in1.Payment_Type_Code))

),

EXPTRANS_LOOKUP_341 AS (

  SELECT 
    in0.REF_HK AS LOOKUP_VARIABLE_7,
    in0.REF_HK AS REF_HK,
    in0.REF_BK AS REF_BK,
    in0.Payment_Type_Group_Name AS Payment_Type_Group_Name,
    in1.Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    in1.Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    in1.Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    in1.Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    in1.Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    in1.GST_Division_Code AS GST_Division_Code,
    in1.Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    in1.CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    in1.Claim_Payment_Description AS Claim_Payment_Description,
    in1.Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    in1.Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    in1.Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    in1.Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    in1.Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    in1.Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    in1.Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    in1.Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    in1.Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    in1.Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    in1.Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    in1.Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    in1.Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    in1.Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    in1.Claim_Payment_Amount AS Claim_Payment_Amount,
    in1.Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    in1.Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    in1.CTL_BATCH_ID AS CTL_BATCH_ID,
    in1.CTL_JOB_ID AS CTL_JOB_ID,
    in1.CTL_LOAD_DM AS CTL_LOAD_DM,
    in1.TRAN_HK AS TRAN_HK,
    in1.TRAN_BK AS TRAN_BK,
    in1.Claim_Tran_Id AS Claim_Tran_Id,
    in1.CLAIM_HK AS CLAIM_HK,
    in1.CLAIM_BK AS CLAIM_BK,
    in1.Company_Code AS Company_Code,
    in1.Claim_Number AS Claim_Number,
    in1.Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    in1.Payment_Type_Code AS Payment_Type_Code,
    in1.Tran_Type_Code AS Tran_Type_Code,
    in1.Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    in1.Claim_RI_Type_Code AS Claim_RI_Type_Code,
    in1.Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    in1.Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    in1.Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    in1.Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    in1.Acct_Period_Code AS Acct_Period_Code,
    in1.Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    in1.Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    in1.Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    in1.LOOKUP_VARIABLE_3 AS LOOKUP_VARIABLE_3,
    in1.LOOKUP_VARIABLE_6 AS LOOKUP_VARIABLE_6,
    in1.LOOKUP_VARIABLE_8 AS LOOKUP_VARIABLE_8,
    in1.LOOKUP_VARIABLE_11 AS LOOKUP_VARIABLE_11,
    in1.LOOKUP_VARIABLE_10 AS LOOKUP_VARIABLE_10,
    in1.LOOKUP_VARIABLE_5 AS LOOKUP_VARIABLE_5,
    in1.LOOKUP_VARIABLE_9 AS LOOKUP_VARIABLE_9,
    in1.LOOKUP_VARIABLE_4 AS LOOKUP_VARIABLE_4
  
  FROM `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_payment_type_group` AS in0
  LEFT JOIN EXPTRANS_LOOKUP_341_LOOKUP_342 AS in1
     ON (CAST(in0.Payment_Type_Group_Name AS INTEGER) = (LTRIM((RTRIM(in1.LOOKUP_VARIABLE_6)))))

),

EXPTRANS AS (

  SELECT 
    TRAN_HK AS TRAN_HK,
    TRAN_BK AS TRAN_BK,
    Claim_Tran_Id AS Claim_Tran_Id,
    CLAIM_HK AS CLAIM_HK,
    CLAIM_BK AS CLAIM_BK,
    Company_Code AS Company_Code,
    Claim_Number AS Claim_Number,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    Payment_Type_Code AS Payment_Type_Code,
    Tran_Type_Code AS Tran_Type_Code,
    Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    Claim_RI_Type_Code AS Claim_RI_Type_Code,
    Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    Acct_Period_Code AS Acct_Period_Code,
    Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    GST_Division_Code AS GST_Division_Code,
    Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    Claim_Payment_Description AS Claim_Payment_Description,
    Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    Claim_Payment_Amount AS Claim_Payment_Amount,
    Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_LOAD_DM AS CTL_LOAD_DM,
    (
      CASE
        WHEN CAST((LOOKUP_VARIABLE_3 IS NULL) AS BOOLEAN)
          THEN LOOKUP_VARIABLE_4
        ELSE LOOKUP_VARIABLE_3
      END
    ) AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    LOOKUP_VARIABLE_7 AS lkp_REF_HK,
    (
      CASE
        WHEN (
          NOT(
            SUBSTRING(
              LOOKUP_VARIABLE_8, 
              ((LOCATE('~', LOOKUP_VARIABLE_8, 0)) + 1), 
              ((LENGTH(LOOKUP_VARIABLE_8)) - (LOCATE('~', LOOKUP_VARIABLE_8, 0)))) IS NULL)
        )
          THEN SUBSTRING(
            LOOKUP_VARIABLE_8, 
            ((LOCATE('~', LOOKUP_VARIABLE_8, 0)) + 1), 
            ((LENGTH(LOOKUP_VARIABLE_8)) - (LOCATE('~', LOOKUP_VARIABLE_8, 0))))
        WHEN (
          NOT(
            SUBSTRING(
              LOOKUP_VARIABLE_9, 
              ((LOCATE('~', LOOKUP_VARIABLE_9, 0)) + 1), 
              ((LENGTH(LOOKUP_VARIABLE_9)) - (LOCATE('~', LOOKUP_VARIABLE_9, 0)))) IS NULL)
        )
          THEN SUBSTRING(
            LOOKUP_VARIABLE_9, 
            ((LOCATE('~', LOOKUP_VARIABLE_9, 0)) + 1), 
            ((LENGTH(LOOKUP_VARIABLE_9)) - (LOCATE('~', LOOKUP_VARIABLE_9, 0))))
        WHEN (
          NOT(
            SUBSTRING(
              LOOKUP_VARIABLE_10, 
              ((LOCATE('~', LOOKUP_VARIABLE_10, 0)) + 1), 
              ((LENGTH(LOOKUP_VARIABLE_10)) - (LOCATE('~', LOOKUP_VARIABLE_10, 0)))) IS NULL)
        )
          THEN SUBSTRING(
            LOOKUP_VARIABLE_10, 
            ((LOCATE('~', LOOKUP_VARIABLE_10, 0)) + 1), 
            ((LENGTH(LOOKUP_VARIABLE_10)) - (LOCATE('~', LOOKUP_VARIABLE_10, 0))))
        WHEN (
          NOT(
            SUBSTRING(
              LOOKUP_VARIABLE_11, 
              ((LOCATE('~', LOOKUP_VARIABLE_11, 0)) + 1), 
              ((LENGTH(LOOKUP_VARIABLE_11)) - (LOCATE('~', LOOKUP_VARIABLE_11, 0)))) IS NULL)
        )
          THEN SUBSTRING(
            LOOKUP_VARIABLE_11, 
            ((LOCATE('~', LOOKUP_VARIABLE_11, 0)) + 1), 
            ((LENGTH(LOOKUP_VARIABLE_11)) - (LOCATE('~', LOOKUP_VARIABLE_11, 0))))
        ELSE NULL
      END
    ) AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    (
      CASE
        WHEN (NOT(SUBSTRING(LOOKUP_VARIABLE_8, 0, ((LOCATE('~', LOOKUP_VARIABLE_8, 0)) - 1)) IS NULL))
          THEN SUBSTRING(LOOKUP_VARIABLE_8, 0, ((LOCATE('~', LOOKUP_VARIABLE_8, 0)) - 1))
        WHEN (NOT(SUBSTRING(LOOKUP_VARIABLE_9, 0, ((LOCATE('~', LOOKUP_VARIABLE_9, 0)) - 1)) IS NULL))
          THEN SUBSTRING(LOOKUP_VARIABLE_9, 0, ((LOCATE('~', LOOKUP_VARIABLE_9, 0)) - 1))
        WHEN (NOT(SUBSTRING(LOOKUP_VARIABLE_10, 0, ((LOCATE('~', LOOKUP_VARIABLE_10, 0)) - 1)) IS NULL))
          THEN SUBSTRING(LOOKUP_VARIABLE_10, 0, ((LOCATE('~', LOOKUP_VARIABLE_10, 0)) - 1))
        WHEN (NOT(SUBSTRING(LOOKUP_VARIABLE_11, 0, ((LOCATE('~', LOOKUP_VARIABLE_11, 0)) - 1)) IS NULL))
          THEN SUBSTRING(LOOKUP_VARIABLE_11, 0, ((LOCATE('~', LOOKUP_VARIABLE_11, 0)) - 1))
        ELSE NULL
      END
    ) AS lkp_REF_HK_tt
  
  FROM EXPTRANS_LOOKUP_341 AS in0

),

FILTRANS_rt AS (

  SELECT * 
  
  FROM EXPTRANS AS in0
  
  WHERE CAST((lkp_REF_HK_tt IS NULL) AS BOOLEAN)

),

EXPTRANS1_LOOKUP_350 AS (

  SELECT 
    in0.CLAIM_TRAN_TYPE_CODE AS LOOKUP_VARIABLE_12,
    in0.REF_HK AS REF_HK,
    in0.REF_BK AS REF_BK,
    in0.CLAIM_TRAN_TYPE_CODE AS CLAIM_TRAN_TYPE_CODE,
    in0.CLAIM_TRAN_TYPE_DESCRIPTION AS CLAIM_TRAN_TYPE_DESCRIPTION,
    in0.CLAIM_TRAN_RECORD_CODE AS CLAIM_TRAN_RECORD_CODE,
    in0.CLAIM_RI_TYPE_CODE AS CLAIM_RI_TYPE_CODE,
    in0.CLAIM_TRAN_MODIFY_TYPE_CODE AS CLAIM_TRAN_MODIFY_TYPE_CODE,
    in0.TRAN_TYPE_CODE AS TRAN_TYPE_CODE,
    in1.Claim_Tran_Id AS Claim_Tran_Id
  
  FROM `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_rt` AS in0
  LEFT JOIN FILTRANS_rt AS in1
     ON (in0.CLAIM_TRAN_RECORD_CODE = in1.Claim_Tran_Record_Code)

),

EXPTRANS1 AS (

  SELECT 
    Claim_Tran_Id AS Claim_Tran_Id,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    LOOKUP_VARIABLE_12 AS lkp_CLAIM_TRAN_TYPE_CODE_rt
  
  FROM EXPTRANS1_LOOKUP_350 AS in0

),

EXPTRANS1_EXPR_326 AS (

  SELECT 
    Claim_Tran_Id AS Claim_Tran_Id_rt,
    lkp_CLAIM_TRAN_TYPE_CODE_rt AS lkp_CLAIM_TRAN_TYPE_CODE_rt,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code
  
  FROM EXPTRANS1 AS in0

),

JNRTRANS AS (

  SELECT 
    in0.lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    in0.Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    in0.Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    in0.CTL_LOAD_DM AS CTL_LOAD_DM,
    in0.Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    in0.Claim_Payment_Amount AS Claim_Payment_Amount,
    in0.Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    in0.Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    in0.Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    in0.CLAIM_BK AS CLAIM_BK,
    in1.lkp_CLAIM_TRAN_TYPE_CODE_rt AS lkp_CLAIM_TRAN_TYPE_CODE_rt,
    in0.Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    in0.Acct_Period_Code AS Acct_Period_Code,
    in0.CTL_BATCH_ID AS CTL_BATCH_ID,
    in0.Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    in0.Claim_Number AS Claim_Number,
    in0.Company_Code AS Company_Code,
    in0.Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    in0.Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    in0.CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    in0.Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    in0.Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    in0.Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    in0.Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    in0.Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    in0.Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    in0.TRAN_HK AS TRAN_HK,
    in0.Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    in1.Claim_Tran_Id_rt AS Claim_Tran_Id_rt,
    in0.Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    in0.lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    in0.CLAIM_HK AS CLAIM_HK,
    in0.Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    in0.Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    in0.Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    in0.Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    in0.Claim_Payment_Description AS Claim_Payment_Description,
    in0.Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    in0.lkp_REF_HK_tt AS lkp_REF_HK_tt,
    in0.TRAN_BK AS TRAN_BK,
    in0.Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    in0.Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    in0.lkp_REF_HK AS lkp_REF_HK,
    in0.Claim_Tran_Id AS Claim_Tran_Id,
    in0.Tran_Type_Code AS Tran_Type_Code,
    in0.Claim_RI_Type_Code AS Claim_RI_Type_Code,
    in0.Payment_Type_Code AS Payment_Type_Code,
    in0.Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    in0.Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    in0.GST_Division_Code AS GST_Division_Code,
    in0.Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    in0.Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    in0.Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    in0.CTL_JOB_ID AS CTL_JOB_ID,
    in0.Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code
  
  FROM EXPTRANS AS in0
  RIGHT JOIN EXPTRANS1_EXPR_326 AS in1
     ON (in1.Claim_Tran_Id_rt = in0.Claim_Tran_Id)

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_mt` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_mt'
    )
  }}

),

JNRTRANS_EXPR_329 AS (

  SELECT 
    lkp_REF_HK_tt AS lkp_REF_HK_tt,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    Tran_Type_Code AS Tran_Type_Code,
    Claim_Tran_Id AS Claim_Tran_Id_mt,
    TRAN_HK AS TRAN_HK,
    TRAN_BK AS TRAN_BK,
    CLAIM_HK AS CLAIM_HK,
    CLAIM_BK AS CLAIM_BK,
    Company_Code AS Company_Code,
    Claim_Number AS Claim_Number,
    Payment_Type_Code AS Payment_Type_Code,
    Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    Claim_RI_Type_Code AS Claim_RI_Type_Code,
    Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    Acct_Period_Code AS Acct_Period_Code,
    Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    GST_Division_Code AS GST_Division_Code,
    Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    Claim_Payment_Description AS Claim_Payment_Description,
    Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    Claim_Payment_Amount AS Claim_Payment_Amount,
    Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_LOAD_DM AS CTL_LOAD_DM,
    lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    lkp_REF_HK AS lkp_REF_HK,
    lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    Claim_Tran_Id_rt AS Claim_Tran_Id_rt,
    lkp_CLAIM_TRAN_TYPE_CODE_rt AS lkp_CLAIM_TRAN_TYPE_CODE_rt
  
  FROM JNRTRANS AS in0

),

FILTRANS_mt AS (

  SELECT * 
  
  FROM JNRTRANS_EXPR_329 AS in0
  
  WHERE CAST((lkp_REF_HK_tt IS NULL) AS BOOLEAN)

),

EXPTRANS2_LOOKUP_merged AS (

  SELECT 
    in0.CLAIM_TRAN_TYPE_CODE AS LOOKUP_VARIABLE_1,
    in2.CLAIM_TRAN_TYPE_CODE AS LOOKUP_VARIABLE_2,
    in2.REF_HK AS REF_HK,
    in2.REF_BK AS REF_BK,
    in2.CLAIM_TRAN_TYPE_CODE AS CLAIM_TRAN_TYPE_CODE,
    in2.CLAIM_TRAN_TYPE_DESCRIPTION AS CLAIM_TRAN_TYPE_DESCRIPTION,
    in2.CLAIM_TRAN_RECORD_CODE AS CLAIM_TRAN_RECORD_CODE,
    in2.CLAIM_RI_TYPE_CODE AS CLAIM_RI_TYPE_CODE,
    in2.CLAIM_TRAN_MODIFY_TYPE_CODE AS CLAIM_TRAN_MODIFY_TYPE_CODE,
    in2.TRAN_TYPE_CODE AS TRAN_TYPE_CODE,
    in1.Claim_Tran_Id_mt AS Claim_Tran_Id_mt
  
  FROM `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_mt` AS in0
  LEFT JOIN FILTRANS_mt AS in1
     ON (
      (in0.CLAIM_TRAN_RECORD_CODE = in1.Claim_Tran_Record_Code)
      AND (in0.TRAN_TYPE_CODE = in1.Tran_Type_Code)
    )
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_REF_CLAIM_TRAN_TYPE_mt` AS in2
     ON (
      (in2.CLAIM_TRAN_RECORD_CODE = in1.Claim_Tran_Record_Code)
      AND (
            in2.TRAN_TYPE_CODE = (
              CASE
                WHEN CAST((in1.Tran_Type_Code IS NULL) AS BOOLEAN)
                  THEN '~'
                ELSE in1.Tran_Type_Code
              END
            )
          )
    )

),

EXPTRANS2 AS (

  SELECT 
    Claim_Tran_Id_mt AS Claim_Tran_Id_mt,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    Tran_Type_Code AS Tran_Type_Code,
    (
      CASE
        WHEN (NOT(LOOKUP_VARIABLE_1 IS NULL))
          THEN LOOKUP_VARIABLE_1
        WHEN (NOT(LOOKUP_VARIABLE_2 IS NULL))
          THEN LOOKUP_VARIABLE_2
        ELSE NULL
      END
    ) AS lkp_CLAIM_TRAN_TYPE_CODE_mt
  
  FROM EXPTRANS2_LOOKUP_merged AS in0

),

JNRTRANS1 AS (

  SELECT 
    in0.Claim_Tran_Id_mt AS Claim_Tran_Id_mt,
    in1.lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    in1.Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    in1.Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    in1.CTL_LOAD_DM AS CTL_LOAD_DM,
    in1.Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    in1.Claim_Payment_Amount AS Claim_Payment_Amount,
    in1.Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    in1.Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    in1.Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    in1.CLAIM_BK AS CLAIM_BK,
    in1.lkp_CLAIM_TRAN_TYPE_CODE_rt AS lkp_CLAIM_TRAN_TYPE_CODE_rt,
    in1.Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    in1.Acct_Period_Code AS Acct_Period_Code,
    in1.CTL_BATCH_ID AS CTL_BATCH_ID,
    in1.Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    in1.Claim_Number AS Claim_Number,
    in1.Company_Code AS Company_Code,
    in1.Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    in1.Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    in1.CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    in1.Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    in1.Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    in1.Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    in1.Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    in1.Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    in1.Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    in1.TRAN_HK AS TRAN_HK,
    in1.Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    in1.Claim_Tran_Id_rt AS Claim_Tran_Id_rt,
    in1.Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    in1.lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    in1.CLAIM_HK AS CLAIM_HK,
    in1.Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    in1.Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    in0.lkp_CLAIM_TRAN_TYPE_CODE_mt AS lkp_CLAIM_TRAN_TYPE_CODE_mt,
    in1.Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    in1.Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    in1.Claim_Payment_Description AS Claim_Payment_Description,
    in1.Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    in1.lkp_REF_HK_tt AS lkp_REF_HK_tt,
    in1.TRAN_BK AS TRAN_BK,
    in1.Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    in1.Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    in1.lkp_REF_HK AS lkp_REF_HK,
    in1.Claim_Tran_Id AS Claim_Tran_Id,
    in1.Tran_Type_Code AS Tran_Type_Code,
    in1.Claim_RI_Type_Code AS Claim_RI_Type_Code,
    in1.Payment_Type_Code AS Payment_Type_Code,
    in1.Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    in1.Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    in1.GST_Division_Code AS GST_Division_Code,
    in1.Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    in1.Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    in1.Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    in1.CTL_JOB_ID AS CTL_JOB_ID,
    in1.Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code
  
  FROM EXPTRANS2 AS in0
  RIGHT JOIN JNRTRANS AS in1
     ON (in0.Claim_Tran_Id_mt = in1.Claim_Tran_Id)

),

JNRTRANS1_GENERATE_SK_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_sk,
    *
  
  FROM JNRTRANS1 AS in0

),

`40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_POL_CLAV_CA` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '40_DM_ORAC_mp_fact_claim_tran_temp', 
      '40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_POL_CLAV_CA'
    )
  }}

),

JNRTRANS1_GENERATE_SK_EXPR_325 AS (

  SELECT 
    CLAIM_BK AS in_CLAIM_BK,
    prophecy_sk AS prophecy_sk,
    Claim_Tran_Id_mt AS Claim_Tran_Id_mt,
    lkp_CLAIM_TRAN_TYPE_CODE_mt AS lkp_CLAIM_TRAN_TYPE_CODE_mt,
    TRAN_HK AS TRAN_HK,
    TRAN_BK AS TRAN_BK,
    Claim_Tran_Id AS Claim_Tran_Id,
    CLAIM_HK AS CLAIM_HK,
    Company_Code AS Company_Code,
    Claim_Number AS Claim_Number,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    Payment_Type_Code AS Payment_Type_Code,
    Tran_Type_Code AS Tran_Type_Code,
    Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    Claim_RI_Type_Code AS Claim_RI_Type_Code,
    Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    Acct_Period_Code AS Acct_Period_Code,
    Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    GST_Division_Code AS GST_Division_Code,
    Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    Claim_Payment_Description AS Claim_Payment_Description,
    Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    Claim_Payment_Amount AS Claim_Payment_Amount,
    Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_LOAD_DM AS CTL_LOAD_DM,
    lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    lkp_REF_HK AS lkp_REF_HK,
    lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    lkp_REF_HK_tt AS lkp_REF_HK_tt,
    Claim_Tran_Id_rt AS Claim_Tran_Id_rt,
    lkp_CLAIM_TRAN_TYPE_CODE_rt AS lkp_CLAIM_TRAN_TYPE_CODE_rt
  
  FROM JNRTRANS1_GENERATE_SK_0 AS in0

),

EXPTRANS3_JOIN_merged AS (

  SELECT 
    in1.EXT_CLAV_ARRANGE_UX_CA AS EXT_CLAV_ARRANGE_UX_CA,
    in2.Claim_Tran_Id_mt AS Claim_Tran_Id_mt,
    in1.EXT_CLAV_BO_TU_CA AS EXT_CLAV_BO_TU_CA,
    in2.lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    in1.EXT_CLAV_CLM_DESC1_CA AS EXT_CLAV_CLM_DESC1_CA,
    in1.EXT_CLAV_DATE_INC_CA AS EXT_CLAV_DATE_INC_CA,
    in2.Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    in2.Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    in1.EXT_CLAV_OTHER_PARTY AS EXT_CLAV_OTHER_PARTY,
    in2.CTL_LOAD_DM AS CTL_LOAD_DM,
    in1.EXT_CLAV_DATE_EXP_CA AS EXT_CLAV_DATE_EXP_CA,
    in2.Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    in2.Claim_Payment_Amount AS Claim_Payment_Amount,
    in2.Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    in2.Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    in2.Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    in1.CLAIM_BK AS CLAIM_BK,
    in1.EXT_CLAV_LIAB_DATE_CA AS EXT_CLAV_LIAB_DATE_CA,
    in1.EXT_CLAV_ICA_CAT_CA AS EXT_CLAV_ICA_CAT_CA,
    in1.EXT_CLAV_GCLM_KEY_CA AS EXT_CLAV_GCLM_KEY_CA,
    in2.lkp_CLAIM_TRAN_TYPE_CODE_rt AS lkp_CLAIM_TRAN_TYPE_CODE_rt,
    in1.EXT_CLAV_VALID_FLAG AS EXT_CLAV_VALID_FLAG,
    in1.EXT_CLAV_INVESTGAT_CA AS EXT_CLAV_INVESTGAT_CA,
    in1.EXT_CLAV_DATE_OCC_CA AS EXT_CLAV_DATE_OCC_CA,
    in1.EXT_CLAV_SALVAGE_CA AS EXT_CLAV_SALVAGE_CA,
    in1.EXT_CLAV_ACCT_KEY_CA AS EXT_CLAV_ACCT_KEY_CA,
    in1.EXT_CLAV_DAM_PERC_CA AS EXT_CLAV_DAM_PERC_CA,
    in1.EXT_CLAV_FRAUD_FLG_CA AS EXT_CLAV_FRAUD_FLG_CA,
    in2.Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    in1.EXT_CLAV_PCODE_L_CA AS EXT_CLAV_PCODE_L_CA,
    in1.EXT_CLAV_TIME_OCC_CA AS EXT_CLAV_TIME_OCC_CA,
    in2.Acct_Period_Code AS Acct_Period_Code,
    in2.CTL_BATCH_ID AS CTL_BATCH_ID,
    in1.EXT_CLAV_CLAM_KEY_CA AS EXT_CLAV_CLAM_KEY_CA,
    in2.Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    in1.EXT_CLAV_CODE AS EXT_CLAV_CODE,
    in1.EXT_CLAV_CLAIMANT_CA AS EXT_CLAV_CLAIMANT_CA,
    in2.Claim_Number AS Claim_Number,
    in1.EXT_CLAV_CCC_NO_CA AS EXT_CLAV_CCC_NO_CA,
    in2.Company_Code AS Company_Code,
    in2.Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    in2.Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    in1.EXT_CLAV_DATE_REPT_CA AS EXT_CLAV_DATE_REPT_CA,
    in1.EXT_CLAV_DATE_NOTIFIED_CA AS EXT_CLAV_DATE_NOTIFIED_CA,
    in2.CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    in1.EXT_CLAV_PAID_CA AS EXT_CLAV_PAID_CA,
    in1.EXT_CLAV_SECT155D_CA AS EXT_CLAV_SECT155D_CA,
    in1.EXT_CLAV_HANDLER_CA AS EXT_CLAV_HANDLER_CA,
    in2.Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    in2.Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    in1.EXT_CLAV_SALVAGE_AMT_CA AS EXT_CLAV_SALVAGE_AMT_CA,
    in2.Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    in1.EXT_CLAV_INC_TU_CA AS EXT_CLAV_INC_TU_CA,
    in2.Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    in1.EXT_CLAV_PAID_TU_CA AS EXT_CLAV_PAID_TU_CA,
    in2.Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    in1.EXT_CLAV_ASSESSR_A_CA AS EXT_CLAV_ASSESSR_A_CA,
    in2.Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    in2.TRAN_HK AS TRAN_HK,
    in2.Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    in1.EXT_CLAV_RISK_KEY_CA AS EXT_CLAV_RISK_KEY_CA,
    in1.EXT_CLAV_RECOVERY_AMT_CA AS EXT_CLAV_RECOVERY_AMT_CA,
    in1.EXT_CLAV_HANDLG_BR_CA AS EXT_CLAV_HANDLG_BR_CA,
    in1.EXT_CLAV_CHANGE_DATE_CA AS EXT_CLAV_CHANGE_DATE_CA,
    in1.EXT_CLAV_VULN_FLAG_CA AS EXT_CLAV_VULN_FLAG_CA,
    in1.EXT_CLAV_SUB_CLASS_CA AS EXT_CLAV_SUB_CLASS_CA,
    in1.EXT_CLAV_DIV_78_DATE_CA AS EXT_CLAV_DIV_78_DATE_CA,
    in1.EXT_CLAV_MAA_SENT_CA AS EXT_CLAV_MAA_SENT_CA,
    in2.Claim_Tran_Id_rt AS Claim_Tran_Id_rt,
    in2.Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    in1.EXT_CLAV_RATE_NCB_CA AS EXT_CLAV_RATE_NCB_CA,
    in1.EXT_CLAV_DESC2_CA AS EXT_CLAV_DESC2_CA,
    in1.EXT_CLAV_RI_PAID_TU_CA AS EXT_CLAV_RI_PAID_TU_CA,
    in1.EXT_CLAV_RI_TRNO_CA AS EXT_CLAV_RI_TRNO_CA,
    in1.EXT_CLAV_CYCL_FLAG_CA AS EXT_CLAV_CYCL_FLAG_CA,
    in2.lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    in1.CLAIM_HK AS CLAIM_HK,
    in2.Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    in1.EXT_CLAV_RI_BO_TU_CA AS EXT_CLAV_RI_BO_TU_CA,
    in1.EXT_CLAV_TERM_ID AS EXT_CLAV_TERM_ID,
    in1.EXT_CLAV_TYPE_UX_CA AS EXT_CLAV_TYPE_UX_CA,
    in2.Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    in1.EXT_CLAV_RI_METHOD_CA AS EXT_CLAV_RI_METHOD_CA,
    in2.lkp_CLAIM_TRAN_TYPE_CODE_mt AS lkp_CLAIM_TRAN_TYPE_CODE_mt,
    in1.EXT_CLAV_CLAM_BRCH_CA AS EXT_CLAV_CLAM_BRCH_CA,
    in1.EXT_CLAV_LAST_EXT_CA AS EXT_CLAV_LAST_EXT_CA,
    in1.EXT_CLAV_BR_TFR_FLG_CA AS EXT_CLAV_BR_TFR_FLG_CA,
    in1.EXT_CLAV_INCURRED_CA AS EXT_CLAV_INCURRED_CA,
    in2.Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    in1.EXT_CLAV_INCURRED_YTD_CA AS EXT_CLAV_INCURRED_YTD_CA,
    in2.Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    in1.EXT_CLAV_STATUS_DATE_CA AS EXT_CLAV_STATUS_DATE_CA,
    in1.EXT_CLAV_CLAIM_STATUS_CA AS EXT_CLAV_CLAIM_STATUS_CA,
    in1.EXT_CLAV_RSK_CLASS_CA AS EXT_CLAV_RSK_CLASS_CA,
    in1.EXT_CLAV_CLNT_KEY_CA AS EXT_CLAV_CLNT_KEY_CA,
    in1.EXT_CLAV_PAID_SH_CA AS EXT_CLAV_PAID_SH_CA,
    in1.EXT_CLAV_TRAN_NO AS EXT_CLAV_TRAN_NO,
    in1.EXT_CLAV_RI_INC_TU_CA AS EXT_CLAV_RI_INC_TU_CA,
    in2.Claim_Payment_Description AS Claim_Payment_Description,
    in0.prophecy_sk AS prophecy_sk,
    in1.EXT_CLAV_INCURRED_SH_CA AS EXT_CLAV_INCURRED_SH_CA,
    in2.Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    in1.EXT_CLAV_GST_CAT_CA AS EXT_CLAV_GST_CAT_CA,
    in1.EXT_CLAV_ESTIMATE_CA AS EXT_CLAV_ESTIMATE_CA,
    in2.lkp_REF_HK_tt AS lkp_REF_HK_tt,
    in2.TRAN_BK AS TRAN_BK,
    in2.Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    in2.Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    in1.EXT_CLAV_DIV_11_XS_CA AS EXT_CLAV_DIV_11_XS_CA,
    in1.EXT_CLAV_VIP_CLAIM_CA AS EXT_CLAV_VIP_CLAIM_CA,
    in0.in_CLAIM_BK AS in_CLAIM_BK,
    in2.lkp_REF_HK AS lkp_REF_HK,
    in1.EXT_CLAV_NON_STD_CA AS EXT_CLAV_NON_STD_CA,
    in1.EXT_CLAV_SOURCE_CO_CA AS EXT_CLAV_SOURCE_CO_CA,
    in1.EXT_CLAV_BAL_OUTS_SH_CA AS EXT_CLAV_BAL_OUTS_SH_CA,
    in1.EXT_CLAV_CLM_INITIAL AS EXT_CLAV_CLM_INITIAL,
    in1.EXT_CLAV_RECOVERY_CA AS EXT_CLAV_RECOVERY_CA,
    in1.EXT_CLAV_BAL_OUTS_CA AS EXT_CLAV_BAL_OUTS_CA,
    in2.Claim_Tran_Id AS Claim_Tran_Id,
    in2.Tran_Type_Code AS Tran_Type_Code,
    in1.EXT_CLAV_DATE_REJECT_CA AS EXT_CLAV_DATE_REJECT_CA,
    in1.EXT_CLAV_DUPL_STS_CA AS EXT_CLAV_DUPL_STS_CA,
    in2.Claim_RI_Type_Code AS Claim_RI_Type_Code,
    in1.EXT_CLAV_PERIOD_TU_CA AS EXT_CLAV_PERIOD_TU_CA,
    in2.Payment_Type_Code AS Payment_Type_Code,
    in1.EXT_CLAV_PREV_LAST_EXT_CA AS EXT_CLAV_PREV_LAST_EXT_CA,
    in2.Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    in2.Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    in1.EXT_CLAV_REOPEN_CA AS EXT_CLAV_REOPEN_CA,
    in2.GST_Division_Code AS GST_Division_Code,
    in2.Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    in1.EXT_CLAV_PAY_BLOCK_CA AS EXT_CLAV_PAY_BLOCK_CA,
    in1.EXT_CLAV_DIV_11_DATE_CA AS EXT_CLAV_DIV_11_DATE_CA,
    in1.EXT_CLAV_CLIENT_CLAM_NO_CA AS EXT_CLAV_CLIENT_CLAM_NO_CA,
    in1.EXT_CLAV_PORTFOLIO_CA AS EXT_CLAV_PORTFOLIO_CA,
    in1.EXT_CLAV_INCURRED_PR_CA AS EXT_CLAV_INCURRED_PR_CA,
    in1.EXT_CLAV_MULT_UNIT_CA AS EXT_CLAV_MULT_UNIT_CA,
    in1.EXT_CLAV_DIV_78_XS_CA AS EXT_CLAV_DIV_78_XS_CA,
    in2.Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    in2.Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    in1.EXT_CLAV_DATE_ACCEPT_CA AS EXT_CLAV_DATE_ACCEPT_CA,
    in2.CTL_JOB_ID AS CTL_JOB_ID,
    in1.EXT_CLAV_SOLICTOR_A_CA AS EXT_CLAV_SOLICTOR_A_CA,
    in1.EXT_CLAV_CLMT_TYPE_CA AS EXT_CLAV_CLMT_TYPE_CA,
    in2.Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    in1.EXT_CLAV_LIAB_STATUS_CA AS EXT_CLAV_LIAB_STATUS_CA
  
  FROM JNRTRANS1_GENERATE_SK_EXPR_325 AS in0
  INNER JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_lkp_POL_CLAV_CA` AS in1
     ON (in1.CLAIM_BK = in0.in_CLAIM_BK)
  INNER JOIN JNRTRANS1_GENERATE_SK_0 AS in2
     ON in0.prophecy_sk = in2.prophecy_sk

),

EXPTRANS4 AS (

  SELECT 
    TRAN_HK AS TRAN_HK,
    TRAN_BK AS TRAN_BK,
    Claim_Tran_Id AS Claim_Tran_Id,
    CLAIM_HK AS CLAIM_HK,
    CLAIM_BK AS CLAIM_BK,
    Company_Code AS Company_Code,
    Claim_Number AS Claim_Number,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    Payment_Type_Code AS Payment_Type_Code,
    Tran_Type_Code AS Tran_Type_Code,
    Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    Claim_RI_Type_Code AS Claim_RI_Type_Code,
    Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    Acct_Period_Code AS Acct_Period_Code,
    Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    GST_Division_Code AS GST_Division_Code,
    Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    Claim_Payment_Description AS Claim_Payment_Description,
    Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    Claim_Payment_Amount AS Claim_Payment_Amount,
    Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_LOAD_DM AS CTL_LOAD_DM,
    lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    lkp_REF_HK AS lkp_REF_HK,
    lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    lkp_REF_HK_tt AS lkp_REF_HK_tt,
    Claim_Tran_Id_rt AS Claim_Tran_Id_rt,
    lkp_CLAIM_TRAN_TYPE_CODE_rt AS lkp_CLAIM_TRAN_TYPE_CODE_rt,
    Claim_Tran_Id_mt AS Claim_Tran_Id_mt,
    lkp_CLAIM_TRAN_TYPE_CODE_mt AS lkp_CLAIM_TRAN_TYPE_CODE_mt,
    EXT_CLAV_DATE_OCC_CA AS EXT_CLAV_DATE_OCC_CA,
    EXT_CLAV_DATE_REPT_CA AS EXT_CLAV_DATE_REPT_CA,
    CAST(NULL AS INTEGER) AS RANKINDEX,
    (
      CASE
        WHEN ((RANKINDEX = 1) AND (lkp_CLAIM_TRAN_TYPE_CODE_tt = 'PAY'))
          THEN Claim_Tran_Gross_Paid_Amount
        ELSE 0
      END
    ) AS out_Claim_Final_Payment_Amount,
    (
      CASE
        WHEN (NOT(lkp_CLAIM_TRAN_TYPE_CODE_tt IS NULL))
          THEN lkp_CLAIM_TRAN_TYPE_CODE_tt
        WHEN (NOT(lkp_CLAIM_TRAN_TYPE_CODE_rt IS NULL))
          THEN lkp_CLAIM_TRAN_TYPE_CODE_rt
        WHEN (NOT(lkp_CLAIM_TRAN_TYPE_CODE_mt IS NULL))
          THEN lkp_CLAIM_TRAN_TYPE_CODE_mt
        ELSE NULL
      END
    ) AS out_CLAIM_TRAN_TYPE_CODE,
    (
      CAST(((Claim_Tran_Date_Key % 10000) / 100) AS INTEGER)
      + (
          (CAST((Claim_Tran_Date_Key / 10000) AS INTEGER) - CAST((EXT_CLAV_DATE_OCC_CA / 10000) AS INTEGER))
          * 12
        )
    ) AS o_Claim_Tran_Accident_Months_Difference_Count,
    (
      CAST(((Claim_Tran_Date_Key % 10000) / 100) AS INTEGER)
      + (
          (CAST((Claim_Tran_Date_Key / 10000) AS INTEGER) - CAST((EXT_CLAV_DATE_REPT_CA / 10000) AS INTEGER))
          * 12
        )
    ) AS o_Claim_Tran_Reported_Months_Difference_Count
  
  FROM EXPTRANS3_JOIN_merged AS in0

),

EXPTRANS5_LOOKUP_merged AS (

  SELECT 
    in0.CLAIM_HK AS LOOKUP_VARIABLE_13,
    in2.Cheq_Reqn_Type_Code AS LOOKUP_VARIABLE_16,
    in3.PARTY_HK AS LOOKUP_VARIABLE_15,
    in4.Payment_Type_Code AS LOOKUP_VARIABLE_14,
    in5.CLAIM_PAYEE_HK AS LOOKUP_VARIABLE_17,
    in5.CLAIM_PAYEE_TRAN_HK AS CLAIM_PAYEE_TRAN_HK,
    in5.CLAIM_PAYEE_HK AS CLAIM_PAYEE_HK,
    in5.CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    in5.CLAIM_PAYEE_BK AS CLAIM_PAYEE_BK,
    in5.CTL_BATCH_ID AS CTL_BATCH_ID,
    in5.CTL_JOB_ID AS CTL_JOB_ID,
    in5.CTL_LOAD_DM AS CTL_LOAD_DM,
    in4.REF_HK AS REF_HK,
    in4.REF_BK AS REF_BK,
    in4.Payment_Type_Code AS Payment_Type_Code,
    in4.Payment_Type_Description AS Payment_Type_Description,
    in3.PARTY_HK AS PARTY_HK,
    in3.PARTY_BK AS PARTY_BK,
    in3.CTL_SRC_CD AS CTL_SRC_CD,
    in2.Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    in2.Cheq_Reqn_Type_Description AS Cheq_Reqn_Type_Description,
    in1.TRAN_HK AS TRAN_HK,
    in1.TRAN_BK AS TRAN_BK,
    in1.Claim_Tran_Id AS Claim_Tran_Id,
    in1.CLAIM_HK AS CLAIM_HK,
    in1.CLAIM_BK AS CLAIM_BK,
    in1.Company_Code AS Company_Code,
    in1.Claim_Number AS Claim_Number,
    in1.Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    in1.Tran_Type_Code AS Tran_Type_Code,
    in1.Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    in1.Claim_RI_Type_Code AS Claim_RI_Type_Code,
    in1.Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    in1.Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    in1.Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    in1.Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    in1.Acct_Period_Code AS Acct_Period_Code,
    in1.Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    in1.Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    in1.Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    in1.Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    in1.Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    in1.Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    in1.Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    in1.Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    in1.GST_Division_Code AS GST_Division_Code,
    in1.Claim_Payment_Description AS Claim_Payment_Description,
    in1.Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    in1.Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    in1.Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    in1.Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    in1.Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    in1.Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    in1.Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    in1.Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    in1.Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    in1.Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    in1.Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    in1.Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    in1.Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    in1.Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    in1.Claim_Payment_Amount AS Claim_Payment_Amount,
    in1.Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    in1.Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    in1.lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    in1.lkp_REF_HK AS lkp_REF_HK,
    in1.lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    in1.out_CLAIM_TRAN_TYPE_CODE AS out_CLAIM_TRAN_TYPE_CODE,
    in1.o_Claim_Tran_Accident_Months_Difference_Count AS o_Claim_Tran_Accident_Months_Difference_Count,
    in1.o_Claim_Tran_Reported_Months_Difference_Count AS o_Claim_Tran_Reported_Months_Difference_Count,
    in0.Claim_Cause_of_Loss_Description AS Claim_Cause_of_Loss_Description,
    in0.Claim_Postcode AS Claim_Postcode,
    in0.Claim_UX_Type_Code AS Claim_UX_Type_Code,
    in0.Claim_UX_Arrangement_Description AS Claim_UX_Arrangement_Description,
    in0.Claim_Salvage_YN AS Claim_Salvage_YN,
    in0.Claim_Recovery_YN AS Claim_Recovery_YN,
    in0.Claim_Recovery_Potential_Amount AS Claim_Recovery_Potential_Amount,
    in0.Claim_Branch_Code AS Claim_Branch_Code,
    in0.Premium_Class_Code AS Premium_Class_Code,
    in0.Product_Code AS Product_Code,
    in0.Claim_Reported_Date_Key AS Claim_Reported_Date_Key,
    in0.Claim_Expiry_Date_Key AS Claim_Expiry_Date_Key,
    in0.Claim_Loss_Date_Key AS Claim_Loss_Date_Key,
    in0.Claim_Indemnity_Decision_Code AS Claim_Indemnity_Decision_Code,
    in0.Claim_Indemnity_Decision_Date_Key AS Claim_Indemnity_Decision_Date_Key,
    in0.Claim_Status_Date_Key AS Claim_Status_Date_Key,
    in0.Claim_Solicitor_Code AS Claim_Solicitor_Code,
    in0.Claim_Assessor_Code AS Claim_Assessor_Code,
    in0.Claim_Investigator_Code AS Claim_Investigator_Code,
    in0.Claim_Handler_Code AS Claim_Handler_Code,
    in0.Claim_Portfolio_Code AS Claim_Portfolio_Code,
    in0.Claim_Reopened_YN AS Claim_Reopened_YN,
    in0.Claim_Notified_Date_Key AS Claim_Notified_Date_Key,
    in0.Claim_Status_Code AS Claim_Status_Code,
    in0.Risk_Class_Code AS Risk_Class_Code,
    in0.Policy_Incep_Date_Key AS Policy_Incep_Date_Key,
    in0.Policy_Period_Name AS Policy_Period_Name,
    in0.Claim_Closed_Date_Key AS Claim_Closed_Date_Key,
    in0.Claim_Non_Standard_Type_Code AS Claim_Non_Standard_Type_Code,
    in0.Claim_Salvage_Amount AS Claim_Salvage_Amount,
    in0.Claim_Gross_Outs_Amount AS Claim_Gross_Outs_Amount,
    in0.Claim_Gross_Paid_Amount AS Claim_Gross_Paid_Amount,
    in0.Claim_Gross_Incurred_Amount AS Claim_Gross_Incurred_Amount,
    in0.Claim_Notify_Days_Count AS Claim_Notify_Days_Count,
    in0.Claim_Liability_Status_Code AS Claim_Liability_Status_Code,
    in0.Claim_Liability_Date_Key AS Claim_Liability_Date_Key,
    in0.Claim_Agg_Arrangement_Type_Key AS Claim_Agg_Arrangement_Type_Key,
    in0.Claim_Agg_Arrangement_Key AS Claim_Agg_Arrangement_Key,
    in0.Claim_BGC_Status_Code AS Claim_BGC_Status_Code,
    in0.Claim_Updated_Date_Key AS Claim_Updated_Date_Key,
    in0.Claim_Updated_User_Code AS Claim_Updated_User_Code,
    in0.Claim_Duplicate_Status_Code AS Claim_Duplicate_Status_Code,
    in0.Claim_Fraud_Type_Code AS Claim_Fraud_Type_Code,
    in0.Claim_Updated_Datetime AS Claim_Updated_Datetime,
    in0.Claim_GROSS_INCURRED_Loss_Type_Code AS Claim_GROSS_INCURRED_Loss_Type_Code,
    in0.Client_Claim_Number AS Client_Claim_Number,
    in0.Claim_Date_Accepted_Key AS Claim_Date_Accepted_Key,
    in0.Claim_Date_Rejected_Key AS Claim_Date_Rejected_Key,
    in0.Claim_CITCE_Percent AS Claim_CITCE_Percent,
    in0.Claim_Vulnerable_Flag AS Claim_Vulnerable_Flag,
    in0.Claim_Time_Occurence AS Claim_Time_Occurence,
    in0.Claim_Cyclone_Flag AS Claim_Cyclone_Flag,
    in0.ARPC_Incident_Number AS ARPC_Incident_Number
  
  FROM `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_CLAIM` AS in0
  LEFT JOIN EXPTRANS4 AS in1
     ON (in0.CLAIM_BK = in1.CLAIM_BK)
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_CHEQ_REQN_TYPE` AS in2
     ON (in2.Cheq_Reqn_Type_Code = in1.Cheq_Reqn_Type_Code)
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_H_PARTY` AS in3
     ON (in3.PARTY_BK = in1.Claim_Tran_RI_Broker_Account_Code)
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_V_S_PAYMENT_TYPE` AS in4
     ON (in4.Payment_Type_Code = in1.Payment_Type_Code)
  RIGHT JOIN `40_DM_ORACmp_fact_claim_tran_temp_SOURCE_LKP_L_CLAIM_PAYEE_TRAN` AS in5
     ON (in5.CLAIM_PAYEE_TRAN_BK = in1.CLAIM_PAYEE_TRAN_BK)

),

EXPTRANS5 AS (

  SELECT 
    TRAN_HK AS TRAN_HK,
    TRAN_BK AS TRAN_BK,
    Claim_Tran_Id AS Claim_Tran_Id,
    CLAIM_HK AS CLAIM_HK,
    CLAIM_BK AS CLAIM_BK,
    Company_Code AS Company_Code,
    Claim_Number AS Claim_Number,
    Claim_Tran_Record_Code AS Claim_Tran_Record_Code,
    Payment_Type_Code AS Payment_Type_Code,
    Tran_Type_Code AS Tran_Type_Code,
    Claim_Tran_Batch_ID AS Claim_Tran_Batch_ID,
    Claim_RI_Type_Code AS Claim_RI_Type_Code,
    Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Code,
    Claim_Tran_RI_Broker_Account_Code AS Claim_Tran_RI_Broker_Account_Code,
    Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    Acct_Period_Code AS Acct_Period_Code,
    Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    GST_Division_Code AS GST_Division_Code,
    Cheq_Reqn_Type_Code AS Cheq_Reqn_Type_Code,
    CLAIM_PAYEE_TRAN_BK AS CLAIM_PAYEE_TRAN_BK,
    Claim_Payment_Description AS Claim_Payment_Description,
    Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    Claim_Payment_Country_Code AS Claim_Payment_Country_Code,
    Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    Claim_Payment_Amount AS Claim_Payment_Amount,
    Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_LOAD_DM AS CTL_LOAD_DM,
    lkp_GST_TOLERANCE_LOW_AMOUNT AS lkp_GST_TOLERANCE_LOW_AMOUNT,
    lkp_REF_HK AS lkp_REF_HK,
    lkp_CLAIM_TRAN_TYPE_CODE_tt AS lkp_CLAIM_TRAN_TYPE_CODE_tt,
    CAST(NULL AS DECIMAL (16, 0)) AS out_Claim_Final_Payment_Amount,
    out_CLAIM_TRAN_TYPE_CODE AS out_CLAIM_TRAN_TYPE_CODE,
    o_Claim_Tran_Accident_Months_Difference_Count AS o_Claim_Tran_Accident_Months_Difference_Count,
    o_Claim_Tran_Reported_Months_Difference_Count AS o_Claim_Tran_Reported_Months_Difference_Count,
    CAST(NULL AS INTEGER) AS RANKINDEX,
    CAST(NULL AS string) AS CLAIM_HK_rnk,
    (
      CASE
        WHEN ((CAST(NULL AS INTEGER) = 1) AND (lkp_CLAIM_TRAN_TYPE_CODE_tt = 'PAY'))
          THEN Claim_Tran_Gross_Paid_Amount
        ELSE 0
      END
    ) AS out_Claim_First_Payment_Amount,
    1 AS o_claim_tran_count,
    (
      CASE
        WHEN ((CAST((RTRIM(Claim_Tran_Id)) AS string) = '') OR (Claim_Tran_Id IS NULL))
          THEN '-'
        ELSE Claim_Tran_Id
      END
    ) AS o_Claim_Tran_Id,
    (
      CASE
        WHEN ((CAST((RTRIM(LOOKUP_VARIABLE_13)) AS string) = '') OR (LOOKUP_VARIABLE_13 IS NULL))
          THEN '-'
        ELSE LOOKUP_VARIABLE_13
      END
    ) AS o_Claim_Key,
    (
      CASE
        WHEN CAST((Acct_Period_Code IS NULL) AS BOOLEAN)
          THEN '0'
        ELSE Acct_Period_Code
      END
    ) AS o_Acct_Period_Code,
    (
      CASE
        WHEN ((CAST((RTRIM(out_CLAIM_TRAN_TYPE_CODE)) AS string) = '') OR (out_CLAIM_TRAN_TYPE_CODE IS NULL))
          THEN '-'
        ELSE out_CLAIM_TRAN_TYPE_CODE
      END
    ) AS o_out_CLAIM_TRAN_TYPE_CODE,
    (
      CASE
        WHEN ((CAST((RTRIM(Claim_Tran_Batch_ID)) AS string) = '') OR (Claim_Tran_Batch_ID IS NULL))
          THEN '-'
        ELSE Claim_Tran_Batch_ID
      END
    ) AS o_Claim_Tran_Batch_ID,
    (
      CASE
        WHEN ((CAST((RTRIM(LOOKUP_VARIABLE_14)) AS string) = '') OR (LOOKUP_VARIABLE_14 IS NULL))
          THEN '-'
        ELSE LOOKUP_VARIABLE_14
      END
    ) AS o_Payment_Type_Key,
    (
      CASE
        WHEN ((CAST((RTRIM(lkp_REF_HK)) AS string) = '') OR (lkp_REF_HK IS NULL))
          THEN '-'
        ELSE lkp_REF_HK
      END
    ) AS o_lkp_REF_HK,
    (
      CASE
        WHEN ((CAST((RTRIM(Tran_Type_Code)) AS string) = '') OR (Tran_Type_Code IS NULL))
          THEN '-'
        ELSE Tran_Type_Code
      END
    ) AS o_Tran_Type_Code,
    (
      CASE
        WHEN ((CAST((RTRIM(Claim_RI_Type_Code)) AS string) = '') OR (Claim_RI_Type_Code IS NULL))
          THEN '-'
        ELSE Claim_RI_Type_Code
      END
    ) AS o_Claim_RI_Type_Code,
    (
      CASE
        WHEN CAST((Claim_Tran_Date_Key IS NULL) AS BOOLEAN)
          THEN 0
        ELSE Claim_Tran_Date_Key
      END
    ) AS o_Claim_Tran_Date_Key,
    (
      CASE
        WHEN ((CAST((RTRIM(Claim_Tran_Modify_Type_Code)) AS string) = '') OR (Claim_Tran_Modify_Type_Code IS NULL))
          THEN '-'
        ELSE Claim_Tran_Modify_Type_Code
      END
    ) AS o_Claim_Tran_Modify_Type_Code,
    (
      CASE
        WHEN ((CAST((RTRIM(LOOKUP_VARIABLE_15)) AS string) = '') OR (LOOKUP_VARIABLE_15 IS NULL))
          THEN '-'
        ELSE LOOKUP_VARIABLE_15
      END
    ) AS o_Claim_Tran_RI_Broker_Account_Key,
    (
      CASE
        WHEN ((CAST((RTRIM(GST_Division_Code)) AS string) = '') OR (GST_Division_Code IS NULL))
          THEN '-'
        ELSE GST_Division_Code
      END
    ) AS o_GST_Division_Code,
    (
      CASE
        WHEN ((CAST((RTRIM(LOOKUP_VARIABLE_16)) AS string) = '') OR (LOOKUP_VARIABLE_16 IS NULL))
          THEN '-'
        ELSE LOOKUP_VARIABLE_16
      END
    ) AS o_Cheq_Reqn_Type_Key,
    (
      CASE
        WHEN ((CAST((RTRIM(LOOKUP_VARIABLE_17)) AS string) = '') OR (LOOKUP_VARIABLE_17 IS NULL))
          THEN '-'
        ELSE LOOKUP_VARIABLE_17
      END
    ) AS o_Claim_Payee_Key,
    (
      CASE
        WHEN ((CAST((RTRIM(Claim_Payment_Description)) AS string) = '') OR (Claim_Payment_Description IS NULL))
          THEN '-'
        ELSE Claim_Payment_Description
      END
    ) AS o_Claim_Payment_Description,
    (
      CASE
        WHEN (
          (CAST((RTRIM(Claim_Tran_Payment_Single_Stage_YN)) AS string) = '')
          OR (Claim_Tran_Payment_Single_Stage_YN IS NULL)
        )
          THEN '-'
        ELSE Claim_Tran_Payment_Single_Stage_YN
      END
    ) AS o_Claim_Tran_Payment_Single_Stage_YN,
    (
      CASE
        WHEN CAST((Claim_Payment_CITCE_Percent IS NULL) AS BOOLEAN)
          THEN 0
        ELSE Claim_Payment_CITCE_Percent
      END
    ) AS o_Claim_Payment_CITCE_Percent,
    (
      CASE
        WHEN CAST((Claim_Payment_GST_Rate IS NULL) AS BOOLEAN)
          THEN 0
        ELSE Claim_Payment_GST_Rate
      END
    ) AS o_Claim_Payment_GST_Rate,
    (
      CASE
        WHEN CAST((o_Claim_Tran_Accident_Months_Difference_Count IS NULL) AS BOOLEAN)
          THEN 0
        ELSE o_Claim_Tran_Accident_Months_Difference_Count
      END
    ) AS o_Claim_Tran_Accident_Months_Difference_Count1,
    (
      CASE
        WHEN CAST((o_Claim_Tran_Reported_Months_Difference_Count IS NULL) AS BOOLEAN)
          THEN 0
        ELSE o_Claim_Tran_Reported_Months_Difference_Count
      END
    ) AS o_Claim_Tran_Reported_Months_Difference_Count1
  
  FROM EXPTRANS5_LOOKUP_merged AS in0

),

EXPTRANS5_EXPR_328 AS (

  SELECT 
    o_Claim_Tran_Id AS Claim_Tran_Id,
    o_Claim_Key AS Claim_Key,
    o_Acct_Period_Code AS Acct_Period_Key,
    o_out_CLAIM_TRAN_TYPE_CODE AS Claim_Tran_Type_Key,
    o_Claim_Tran_Batch_ID AS Claim_Tran_Batch_Id,
    o_Payment_Type_Key AS Payment_Type_Key,
    o_lkp_REF_HK AS Payment_Type_Group_Key,
    o_Tran_Type_Code AS Tran_Type_Key,
    o_Claim_RI_Type_Code AS Claim_RI_Type_Key,
    o_Claim_Tran_Date_Key AS Claim_Tran_Date_Key,
    Claim_Tran_Last_Modified_Datetime AS Claim_Tran_Last_Modified_Datetime,
    o_Claim_Tran_Modify_Type_Code AS Claim_Tran_Modify_Type_Key,
    o_Claim_Tran_RI_Broker_Account_Key AS Claim_Tran_RI_Broker_Account_Key,
    o_GST_Division_Code AS GST_Division_Key,
    o_Cheq_Reqn_Type_Key AS Cheq_Reqn_Type_Key,
    o_Claim_Payee_Key AS Claim_Payee_Key,
    o_Claim_Payment_Description AS Claim_Payment_Description,
    o_Claim_Tran_Payment_Single_Stage_YN AS Claim_Tran_Payment_Single_Stage_YN,
    o_claim_tran_count AS Claim_Tran_Count,
    Claim_Tran_Ground_Up_Incurred_Amount AS Claim_Tran_Ground_Up_Incurred_Amount,
    Claim_Tran_Ground_Up_Outs_Amount AS Claim_Tran_Ground_Up_Outs_Amount,
    Claim_Tran_Ground_Up_Paid_Amount AS Claim_Tran_Ground_Up_Paid_Amount,
    Claim_Tran_UX_Incurred_Amount AS Claim_Tran_UX_Incurred_Amount,
    Claim_Tran_UX_Outs_Amount AS Claim_Tran_UX_Outs_Amount,
    Claim_Tran_UX_Paid_Amount AS Claim_Tran_UX_Paid_Amount,
    Claim_Tran_Gross_Incurred_Amount AS Claim_Tran_Gross_Incurred_Amount,
    Claim_Tran_Gross_Outs_Amount AS Claim_Tran_Gross_Outs_Amount,
    Claim_Tran_Gross_Paid_Amount AS Claim_Tran_Gross_Paid_Amount,
    Claim_Tran_RI_Incurred_Amount AS Claim_Tran_RI_Incurred_Amount,
    Claim_Tran_RI_Outs_Amount AS Claim_Tran_RI_Outs_Amount,
    Claim_Tran_RI_Paid_Amount AS Claim_Tran_RI_Paid_Amount,
    Claim_Tran_Net_Incurred_Amount AS Claim_Tran_Net_Incurred_Amount,
    Claim_Tran_Net_Outs_Amount AS Claim_Tran_Net_Outs_Amount,
    Claim_Tran_Net_Paid_Amount AS Claim_Tran_Net_Paid_Amount,
    Claim_Tran_Recovery_Amount AS Claim_Tran_Recovery_Amount,
    o_Claim_Payment_CITCE_Percent AS Claim_Payment_CITCE_Percent,
    Claim_Payment_Amount AS Claim_Payment_Amount,
    o_Claim_Payment_GST_Rate AS Claim_Payment_GST_Rate,
    Claim_Payment_GST_Expected_Amount AS Claim_Payment_GST_Expected_Amount,
    Claim_Payment_GST_Claimed_Amount AS Claim_Payment_GST_Claimed_Amount,
    Claim_Payment_GST_Difference_Amount AS Claim_Payment_GST_Difference_Amount,
    lkp_GST_TOLERANCE_LOW_AMOUNT AS Claim_Payment_GST_Tolerance_Amount,
    Claim_Payment_Incl_GST_Amount AS Claim_Payment_Incl_GST_Amount,
    o_Claim_Tran_Accident_Months_Difference_Count1 AS Claim_Tran_Accident_Months_Difference_Count,
    o_Claim_Tran_Reported_Months_Difference_Count1 AS Claim_Tran_Reported_Months_Difference_Count,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_LOAD_DM AS CTL_LOAD_DM,
    out_Claim_First_Payment_Amount AS Claim_Final_Payment_Amount,
    out_Claim_Final_Payment_Amount AS Claim_First_Payment_Amount
  
  FROM EXPTRANS5 AS in0

)

SELECT *

FROM EXPTRANS5_EXPR_328
