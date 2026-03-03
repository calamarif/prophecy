{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH `15_RDV_POLmp_log_audit_info_AUDIT_TASK_PARAM` AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('15_RDV_POL_mp_log_audit_info_1', '15_RDV_POLmp_log_audit_info_AUDIT_TASK_PARAM') }}

),

EXPTRANS AS (

  SELECT 
    JOB_NAME AS JOB_NAME,
    TASK_NAME AS TASK_NAME,
    TARGET_OBJECT_NAME AS TARGET_OBJECT_NAME,
    METRICS_QUERY AS METRICS_QUERY1,
    ACTIVE_FLAG AS ACTIVE_FLAG,
    CTL_BATCH_ID AS CTL_BATCH_ID,
    CTL_INTRADAY_ID AS CTL_INTRADAY_ID,
    CTL_JOB_RUN_ID AS CTL_JOB_RUN_ID,
    CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    CASE
      WHEN (
        (isnull({{ var('METRICS_QUERY') }}) OR ({{ var('METRICS_QUERY') }} = 'NA'))
        OR ({{ var('METRICS_QUERY') }} = '')
      )
        THEN concat(
          'SELECT  SRC.SUBJECT_AREA,SRC.WORKFLOW_ID,SRC.WORKFLOW_NAME ,SRC.SESSION_ID,SRC.SESSION_NAME ,', 
          'coalesce(TGT.TABLE_NAME,', 
          '\'', 
          'NA', 
          '\'', 
          ') ', 
          ',SRC.WORKFLOW_RUN_ID ,SUCCESSFUL_SOURCE_ROWS ,FAILED_SOURCE_ROWS ', 
          ', FILTERED_SOURCE_ROWS ', 
          ',coalesce(TGT.SUCCESSFUL_ROWS,src.SUCCESSFUL_ROWS) SUCCESSFUL_ROWS ', 
          ',coalesce(TGT.FAILED_ROWS,src.FAILED_ROWS) FAILED_ROWS ', 
          ',coalesce(TGT.FILTERED_ROWS,src.tgt_FILTERED_ROWS) FILTERED_ROWS ', 
          ',SRC.ACTUAL_START,SRC.SESSION_TIMESTAMP,THROUGHPUT,SRC.FIRST_ERROR_MSG ', 
          ' from ', 
          '(SELECT  SUBJECT_AREA,WORKFLOW_NAME ,session_id,SESSION_NAME , workflow_id,WORKFLOW_RUN_ID ,SUCCESSFUL_SOURCE_ROWS ,FAILED_SOURCE_ROWS , ', 
          {{ var('FILTERED_SOURCE_ROWS') }}, 
          ' AS FILTERED_SOURCE_ROWS ', 
          ' ,SUCCESSFUL_ROWS ,FAILED_ROWS, ', 
          {{ var('FILTERED_SOURCE_ROWS') }}, 
          ' as tgt_FILTERED_ROWS ', 
          ' ,ACTUAL_START,SESSION_TIMESTAMP,FIRST_ERROR_MSG ', 
          ' from (select  a.*,row_number() over (partition by SUBJECT_id,workflow_id, SESSION_ID order by actual_start desc) as id ', 
          ' from REP_SESS_LOG a where subject_area== ', 
          '\'', 
          {{ var('PMFolderName') }}, 
          '\'', 
          ' and WORKFLOW_NAME ==  ', 
          '\'', 
          {{ var('PMWorkflowName') }}, 
          '\'', 
          ') B where id==1) src ', 
          ' left join', 
          '(SELECT  SUBJECT_AREA, workflow_id,session_id, TABLE_NAME,SUM(SUCCESSFUL_ROWS) SUCCESSFUL_ROWS ,SUM(FAILED_ROWS) FAILED_ROWS , ', 
          {{ var('FILTERED_SOURCE_ROWS') }}, 
          ' AS FILTERED_ROWS', 
          ',avg(THROUGHPUT) THROUGHPUT', 
          ' from ', 
          '( select  a.*,row_number() over (partition by SUBJECT_id,workflow_id, SESSION_ID order by START_TIME desc) as id ', 
          ' from REP_SESS_tbl_LOG a where subject_area==', 
          '\'', 
          {{ var('PMFolderName') }}, 
          '\'', 
          ') B where id==1', 
          ' GROUP BY SUBJECT_AREA,workflow_id ,session_id , TABLE_NAME', 
          ' ) as TGT', 
          ' on SRC.WORKFLOW_ID==TGT.WORKFLOW_ID', 
          ' and  SRC.session_id==TGT.session_id', 
          ' where session_name not in (', 
          {{ var('exclude_sessions') }}, 
          ' )')
      ELSE {{ var('METRICS_QUERY') }}
    END AS metrics_query
  
  FROM `15_RDV_POLmp_log_audit_info_AUDIT_TASK_PARAM` AS in0

)

SELECT *

FROM EXPTRANS
