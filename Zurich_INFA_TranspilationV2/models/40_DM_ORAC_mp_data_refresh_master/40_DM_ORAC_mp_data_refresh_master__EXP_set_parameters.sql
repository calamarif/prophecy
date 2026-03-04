{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH DUMMY_SOURCE AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('40_DM_ORAC_mp_data_refresh_master', 'DUMMY_SOURCE') }}

),

EXP_set_parameters AS (

  SELECT 
    DUMMY AS DUMMY,
    {{ var('CTL_BATCH_ID') }} AS ctl_batch_id,
    {{ var('MAXIMUM_DEGREE_OF_PARALLELISM') }} AS maximum_degree_of_parallelism,
    {{ var('TARGET_SCHEMA_LIST') }} AS target_schema_list,
    CAST({{ var('RETRY_FAILED_STEPS') }} AS string) AS retry_failed_steps
  
  FROM DUMMY_SOURCE AS in0

)

SELECT *

FROM EXP_set_parameters
