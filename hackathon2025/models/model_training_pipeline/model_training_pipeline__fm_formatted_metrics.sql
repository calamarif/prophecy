{{
  config({    
    "materialized": "ephemeral",
    "database": "westpac",
    "schema": "callum"
  })
}}

WITH churn_model_training AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('model_training_pipeline', 'churn_model_training') }}

),

fm_formatted_metrics AS (

  SELECT * 
  
  FROM churn_model_training

)

SELECT *

FROM fm_formatted_metrics
