{{
  config({    
    "materialized": "table",
    "alias": "sh_DIM_GI_CLAIM_STATUS_HISTORY_MTH_AGG",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg', 
      '35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION'
    )
  }}

),

`35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_SOURCE_sh_LKP_Dim_Cal_Month_ID_Range` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg', 
      '35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_SOURCE_sh_LKP_Dim_Cal_Month_ID_Range'
    )
  }}

),

`35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_sh_DIM_GI_CLAIM` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      '35_SDM_mp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg', 
      '35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_sh_DIM_GI_CLAIM'
    )
  }}

),

SQ_sh_DIM_GI_CLAIM AS (

  SELECT 
    CLAIM_ID AS CLAIM_ID,
    REC_STRT_DATE AS REC_STRT_DATE,
    CLAIM_STATUS_CODE AS IN_CLAIM_STATUS_CODE,
    REC_END_DATE AS REC_END_DATE,
    DATE_RPTED AS IN_DATE_RPTED,
    BUSN_AREA_NAME AS BUSN_AREA_NAME,
    RSK_ID AS RSK_ID,
    RI_METHOD_CODE AS RI_METHOD_CODE,
    DATE_OF_STATUS AS DATE_OF_STATUS,
    DATE_NOTIFIED AS DATE_NOTIFIED,
    DATE_RJCTED AS DATE_RJCTED,
    DATE_ACPTED AS DATE_ACPTED,
    LOSS_PRD_CODE AS LOSS_PRD_CODE,
    CLAIM_TYPE_CODE AS CLAIM_TYPE_CODE,
    INVESTIGATOR_CODE AS INVESTIGATOR_CODE,
    ASSESSOR_CODE AS ASSESSOR_CODE,
    SOLICITOR_CODE AS SOLICITOR_CODE,
    MAA_SENT_MTH_ID AS MAA_SENT_MTH_ID,
    CLAIM_FRAUD_CODE AS CLAIM_FRAUD_CODE,
    SUBROGATION_FLG AS SUBROGATION_FLG,
    REOPENED_FLG AS REOPENED_FLG,
    PMT_BLOCK_FLG AS PMT_BLOCK_FLG,
    BRANCH_TRANF_FLG AS BRANCH_TRANF_FLG,
    CLSD_DATE AS CLSD_DATE,
    DATE_OF_LOSS AS DATE_OF_LOSS,
    TRANS_TS AS TRANS_TS,
    CLAIM_NUM AS CLAIM_NUM,
    CMPY_NUM AS CMPY_NUM,
    DISP_CLAIM_NUM AS DISP_CLAIM_NUM,
    CLAIMANT_NAME AS CLAIMANT_NAME,
    CLIENT_CLAIM_NUM AS CLIENT_CLAIM_NUM,
    ICA_CAT_CODE AS ICA_CAT_CODE,
    CAUSE_OF_LOSS AS CAUSE_OF_LOSS,
    BODILY_INJURY_FLG AS BODILY_INJURY_FLG,
    CAST(NULL AS string) AS CLAIM_OFFICER_CODE,
    UW_CODE AS UW_CODE,
    RATE_NSB_FLG AS RATE_NSB_FLG,
    OTHER_PARTY_FLG AS OTHER_PARTY_FLG,
    MUTI_UNIT_FLG AS MUTI_UNIT_FLG,
    VIP_CLAIM_FLG AS VIP_CLAIM_FLG,
    DAMAGE_PERC AS DAMAGE_PERC,
    CLAIM_SRC_CODE AS CLAIM_SRC_CODE,
    UNDER_EXCESS_TYPE AS UNDER_EXCESS_TYPE,
    GST_CAT AS GST_CAT,
    DVSN_11_EXCESS_FLG AS DVSN_11_EXCESS_FLG,
    DVSN_78_FLG AS DVSN_78_FLG,
    DVSN_11_EXCESS_DATE AS DVSN_11_EXCESS_DATE,
    DVSN_78_EXCESS_DATE AS DVSN_78_EXCESS_DATE,
    MSTR_CLAIM_NUM AS MSTR_CLAIM_NUM,
    CLAIM_CAUSE_CODE AS CLAIM_CAUSE_CODE,
    CTL_SRC_ROW_ID AS CTL_SRC_ROW_ID,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_JOB_ID AS CTL_JOB_ID,
    CTL_EXTRACT_ID AS CTL_EXTRACT_ID,
    CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    DEFAULT_PREM_CLASS_CODE AS DEFAULT_PREM_CLASS_CODE,
    COVR_INCEPTION_DATE AS COVR_INCEPTION_DATE,
    COVR_EXPIRY_DATE AS COVR_EXPIRY_DATE,
    POL_ID AS POL_ID,
    CLAIM_BRANCH_CODE AS CLAIM_BRANCH_CODE,
    CTL_REC_UPDATE_DATE AS CTL_REC_UPDATE_DATE,
    (monotonically_increasing_id()) AS prophecy_sk
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_sh_DIM_GI_CLAIM` AS in0

),

EXP_Group_Claims_By_Status_Code_LOOKUP_275 AS (

  SELECT 
    in1.CLAIM_ID AS LOOKUP_VARIABLE_1,
    in0.CLAIM_ID AS CLAIM_ID,
    in0.TRANS_DATE AS TRANS_DATE,
    in1.REC_STRT_DATE AS REC_STRT_DATE,
    in1.IN_CLAIM_STATUS_CODE AS IN_CLAIM_STATUS_CODE,
    in1.REC_END_DATE AS REC_END_DATE,
    in1.IN_DATE_RPTED AS IN_DATE_RPTED,
    in1.prophecy_sk AS prophecy_sk
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_SOURCE_LKP_DATA_GI_CLAIM_TRANSACTION` AS in0
  LEFT JOIN SQ_sh_DIM_GI_CLAIM AS in1
     ON (in0.CLAIM_ID = in1.CLAIM_ID)
    and (in0.TRANS_DATE >= lag(in1.REC_STRT_DATE) OVER (PARTITION BY 1 ORDER BY 1 ASC NULLS FIRST))
    and (in0.TRANS_DATE <= in1.REC_STRT_DATE)

),

EXP_Group_Claims_By_Status_Code AS (

  SELECT 
    CLAIM_ID AS CLAIM_ID,
    REC_STRT_DATE AS REC_STRT_DATE,
    REC_END_DATE AS REC_END_DATE,
    (TO_TIMESTAMP(IN_DATE_RPTED, 'yyyyMMdd')) AS DATE_REPORTED_ID,
    (
      CASE
        WHEN (CLAIM_ID = (LAG(CLAIM_ID) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)))
          THEN (
            CASE
              WHEN (
                (
                  CAST((
                    LAG(
                      (
                        CASE
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'A')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'C')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'F')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'O')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'R')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'N')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'D')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'P')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'Y')
                            THEN 'C'
                          ELSE 'C'
                        END
                      )) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)
                  ) AS string) = 'C'
                )
                AND (
                      (
                        CASE
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'A')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'C')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'F')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'O')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'R')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'N')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'D')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'P')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'Y')
                            THEN 'C'
                          ELSE 'C'
                        END
                      ) = 'A'
                    )
              )
                THEN 1
              WHEN (
                (
                  CAST((
                    LAG(
                      (
                        CASE
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'A')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'C')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'F')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'O')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'R')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'N')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'D')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'P')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'Y')
                            THEN 'C'
                          ELSE 'C'
                        END
                      )) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)
                  ) AS string) = 'C'
                )
                AND (
                      (
                        CASE
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'A')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'C')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'F')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'O')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'R')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'N')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'D')
                            THEN 'C'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'P')
                            THEN 'A'
                          WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'Y')
                            THEN 'C'
                          ELSE 'C'
                        END
                      ) = 'C'
                    )
              )
                THEN (
                  CASE
                    WHEN (NOT(LOOKUP_VARIABLE_1 IS NULL))
                      THEN 1
                    ELSE 0
                  END
                )
              ELSE 0
            END
          )
        ELSE 0
      END
    ) AS REOPEN_COUNT,
    (
      CASE
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'A')
          THEN 'A'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'C')
          THEN 'C'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'F')
          THEN 'C'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'O')
          THEN 'A'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'R')
          THEN 'A'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'N')
          THEN 'A'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'D')
          THEN 'C'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'P')
          THEN 'A'
        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'Y')
          THEN 'C'
        ELSE 'C'
      END
    ) AS CLAIM_STATUS_CODE,
    (
      CASE
        WHEN (
          (CLAIM_ID = (LAG(CLAIM_ID) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)))
          AND (
                (
                  CASE
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'A')
                      THEN 'A'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'C')
                      THEN 'C'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'F')
                      THEN 'C'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'O')
                      THEN 'A'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'R')
                      THEN 'A'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'N')
                      THEN 'A'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'D')
                      THEN 'C'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'P')
                      THEN 'A'
                    WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'Y')
                      THEN 'C'
                    ELSE 'C'
                  END
                ) = (
                  LAG(
                    (
                      CASE
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'A')
                          THEN 'A'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'C')
                          THEN 'C'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'F')
                          THEN 'C'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'O')
                          THEN 'A'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'R')
                          THEN 'A'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'N')
                          THEN 'A'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'D')
                          THEN 'C'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'P')
                          THEN 'A'
                        WHEN (CAST(SUBSTRING(UPPER(IN_CLAIM_STATUS_CODE), 1, 1) AS string) = 'Y')
                          THEN 'C'
                        ELSE 'C'
                      END
                    )) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)
                )
              )
        )
          THEN 1
        ELSE 0
      END
    ) AS Consecutive_Same_Claim_Status,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Group_Claims_By_Status_Code_LOOKUP_275 AS in0

),

EXP_Get_Start_Mth_ID AS (

  SELECT 
    (TO_TIMESTAMP(REC_STRT_DATE, 'yyyyMM')) AS REC_STRT_MTH_ID,
    (
      DATEDIFF(
        REC_STRT_DATE, 
        (
          TO_TIMESTAMP(
            (
              CONCAT(
                SUBSTRING(CAST(DATE_REPORTED_ID AS string), 1, 4), 
                '-', 
                SUBSTRING(CAST(DATE_REPORTED_ID AS string), 5, 2), 
                '-', 
                SUBSTRING(CAST(DATE_REPORTED_ID AS string), 7, 2), 
                ' 00:00:00')
            ), 
            'yyyy-MM-dd HH:mm:ss')
        ))
    ) AS DAYS_TO_CLOSE,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_Group_Claims_By_Status_Code AS in0

),

AGG_Pick_latest_with_Reopen_Count_expr_JOIN AS (

  SELECT 
    in1.REC_STRT_MTH_ID AS REC_STRT_MTH_ID,
    in0.REC_STRT_DATE AS REC_STRT_DATE,
    in1.DAYS_TO_CLOSE AS DAYS_TO_CLOSE,
    in0.CLAIM_ID AS CLAIM_ID,
    in0.Consecutive_Same_Claim_Status AS Consecutive_Same_Claim_Status,
    in0.REOPEN_COUNT AS REOPEN_COUNT,
    in0.prophecy_sk AS prophecy_sk,
    in0.REC_END_DATE AS REC_END_DATE,
    in0.CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE
  
  FROM EXP_Group_Claims_By_Status_Code AS in0
  INNER JOIN EXP_Get_Start_Mth_ID AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

AGG_Pick_latest_with_Reopen_Count AS (

  SELECT 
    first(REC_STRT_DATE) AS REC_STRT_DATE,
    first(CLAIM_STATUS_CODE) AS CLAIM_STATUS_CODE,
    first(REC_END_DATE) AS REC_END_DATE,
    SUM(REOPEN_COUNT) AS SUM_REOPEN_COUNT,
    first(DAYS_TO_CLOSE) AS DAYS_TO_CLOSE,
    first(Consecutive_Same_Claim_Status) AS Consecutive_Same_Claim_Status,
    first(CLAIM_ID) AS CLAIM_ID,
    first(REC_STRT_MTH_ID) AS REC_STRT_MTH_ID
  
  FROM AGG_Pick_latest_with_Reopen_Count_expr_JOIN AS in0
  
  GROUP BY 
    CLAIM_ID, REC_STRT_MTH_ID

),

EXP_Remove_Consecutive_Closed_Claims_Accross_Months AS (

  SELECT 
    CLAIM_ID AS CLAIM_ID,
    REC_STRT_MTH_ID AS REC_STRT_MTH_ID,
    REC_STRT_DATE AS REC_STRT_DATE,
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    REC_END_DATE AS REC_END_DATE,
    SUM_REOPEN_COUNT AS REOPEN_COUNT,
    DAYS_TO_CLOSE AS DAYS_TO_CLOSE,
    1 AS JOIN_DUMMY_SRC,
    Consecutive_Same_Claim_Status AS Consecutive_Same_Claim_Status
  
  FROM AGG_Pick_latest_with_Reopen_Count AS in0

),

FIL_Remove_Consecutive_Closed_Records AS (

  SELECT * 
  
  FROM EXP_Remove_Consecutive_Closed_Claims_Accross_Months AS in0
  
  WHERE CAST((NOT((Consecutive_Same_Claim_Status = 1) AND (CLAIM_STATUS_CODE = 'C'))) AS BOOLEAN)

),

JNR_Max_Rec_Strt_Date AS (

  SELECT 
    in0.REC_STRT_MTH_ID AS REC_STRT_MTH_ID,
    in0.JOIN_DUMMY_SRC AS JOIN_DUMMY_SRC,
    in0.REC_STRT_DATE AS REC_STRT_DATE,
    in0.DAYS_TO_CLOSE AS DAYS_TO_CLOSE,
    in0.CLAIM_ID AS CLAIM_ID,
    in0.REOPEN_COUNT AS REOPEN_COUNT,
    in1.MAX_REC_STRT_DATE AS MAX_REC_STRT_DATE,
    in0.REC_END_DATE AS REC_END_DATE,
    in0.CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    in1.JOIN_DUMMY_MAX_ST_DT AS JOIN_DUMMY_MAX_ST_DT
  
  FROM FIL_Remove_Consecutive_Closed_Records AS in0
  INNER JOIN `35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_sh_DIM_GI_CLAIM` AS in1
     ON (in1.JOIN_DUMMY_MAX_ST_DT = in0.JOIN_DUMMY_SRC)

),

EXP_Remaining_Properties AS (

  SELECT 
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    (
      CASE
        WHEN (CLAIM_STATUS_CODE = 'A')
          THEN 'Y'
        ELSE 'N'
      END
    ) AS ACTIVE_FLG_YN,
    REOPEN_COUNT AS COUNT_REOPENED,
    DAYS_TO_CLOSE AS DAYS_TO_CLOSE,
    REC_STRT_MTH_ID AS REC_STRT_MTH_ID,
    REC_END_DATE AS REC_END_DATE,
    MAX_REC_STRT_DATE AS MAX_REC_STRT_DATE,
    CASE
      WHEN (CLAIM_STATUS_CODE = 'C')
        THEN CAST(to_timestamp(REC_STRT_DATE, 'yyyyMM') AS INT)
      WHEN (to_timestamp(REC_END_DATE, 'yyyyMMdd') = '99991231')
        THEN CAST(to_timestamp(MAX_REC_STRT_DATE, 'yyyyMM') AS INT)
      WHEN (REC_END_DATE = to_timestamp(concat(to_timestamp(last_day(REC_END_DATE), 'yyyyMMdd'), ' 23:59:59'), 'yyyyMMdd HH:mm:ss'))
        THEN CAST(to_timestamp(REC_END_DATE, 'yyyyMM') AS INT)
      ELSE CAST(to_timestamp(add_months(REC_END_DATE, -1), 'yyyyMM') AS INT)
    END AS MAX_REPORT_END_MTH_ID,
    CLAIM_ID AS CLAIM_ID,
    REC_STRT_DATE AS REC_STRT_DATE
  
  FROM JNR_Max_Rec_Strt_Date AS in0

),

EXP_Remaining_Properties_EXPR_272 AS (

  SELECT 
    REC_STRT_MTH_ID AS IN_START_MTH_ID,
    MAX_REPORT_END_MTH_ID AS IN_END_MTH_ID,
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    ACTIVE_FLG_YN AS ACTIVE_FLG_YN,
    COUNT_REOPENED AS COUNT_REOPENED,
    DAYS_TO_CLOSE AS DAYS_TO_CLOSE,
    REC_END_DATE AS REC_END_DATE,
    MAX_REC_STRT_DATE AS MAX_REC_STRT_DATE,
    CLAIM_ID AS CLAIM_ID,
    REC_STRT_DATE AS REC_STRT_DATE
  
  FROM EXP_Remaining_Properties AS in0

),

sh_LKP_Dim_Cal_Month_ID_Range_JOIN AS (

  SELECT 
    in0.MTH_ID AS MTH_ID,
    in1.IN_START_MTH_ID AS IN_START_MTH_ID,
    in1.IN_END_MTH_ID AS IN_END_MTH_ID
  
  FROM `35_SDMmp_mdl_sdm_Dim_GI_Claim_Status_History_Mth_Agg_SOURCE_sh_LKP_Dim_Cal_Month_ID_Range` AS in0
  INNER JOIN EXP_Remaining_Properties_EXPR_272 AS in1
     ON (
      (in0.MTH_ID >= CAST(in1.IN_START_MTH_ID AS INTEGER))
      AND (in0.MTH_ID <= CAST(in1.IN_END_MTH_ID AS INTEGER))
    )

),

AGG_Distinct_Month_Ranges AS (

  SELECT * 
  
  FROM sh_LKP_Dim_Cal_Month_ID_Range_JOIN AS in0

),

EXP_Remaining_Properties_EXPR_270 AS (

  SELECT 
    REC_STRT_MTH_ID AS REC_STRT_MTH_ID,
    ACTIVE_FLG_YN AS ACTIVE_FLG_YN,
    COUNT_REOPENED AS COUNT_REOPENED,
    CLAIM_ID AS CLAIM_ID,
    MAX_REPORT_END_MTH_ID AS REC_END_MTH_ID,
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    DAYS_TO_CLOSE AS DAYS_TO_CLOSE,
    REC_END_DATE AS REC_END_DATE,
    MAX_REC_STRT_DATE AS MAX_REC_STRT_DATE,
    REC_STRT_DATE AS REC_STRT_DATE
  
  FROM EXP_Remaining_Properties AS in0

),

JNR_Month_Range AS (

  SELECT 
    in0.REC_STRT_MTH_ID AS REC_STRT_MTH_ID,
    in0.REC_END_MTH_ID AS REC_END_MTH_ID,
    in1.MTH_ID AS MTH_ID,
    in0.DAYS_TO_CLOSE AS DAYS_TO_CLOSE,
    in1.IN_START_MTH_ID AS IN_START_MTH_ID,
    in0.CLAIM_ID AS CLAIM_ID,
    in0.ACTIVE_FLG_YN AS ACTIVE_FLG_YN,
    in1.IN_END_MTH_ID AS IN_END_MTH_ID,
    in0.CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    in0.COUNT_REOPENED AS COUNT_REOPENED
  
  FROM EXP_Remaining_Properties_EXPR_270 AS in0
  INNER JOIN AGG_Distinct_Month_Ranges AS in1
     ON ((in1.IN_START_MTH_ID = in0.REC_STRT_MTH_ID) AND (in1.IN_END_MTH_ID = in0.REC_END_MTH_ID))

),

EXP_Modify_Reopen_Count AS (

  SELECT 
    CLAIM_ID AS CLAIM_ID,
    MTH_ID AS STATUS_MTH_ID,
    CLAIM_STATUS_CODE AS CLAIM_STATUS_CODE,
    ACTIVE_FLG_YN AS ACTIVE_FLG_YN,
    (
      CASE
        WHEN (MTH_ID = REC_STRT_MTH_ID)
          THEN COUNT_REOPENED
        ELSE 0
      END
    ) AS COUNT_REOPENED,
    DAYS_TO_CLOSE AS DAYS_TO_CLOSE
  
  FROM JNR_Month_Range AS in0

)

SELECT *

FROM EXP_Modify_Reopen_Count
