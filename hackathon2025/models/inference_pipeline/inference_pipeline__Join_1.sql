{{
  config({    
    "materialized": "ephemeral",
    "database": "westpac",
    "schema": "callum"
  })
}}

WITH inf_telecom_customer_churn AS (

  SELECT * 
  
  FROM {{ source('westpac_callum', 'telecom_customer_churn') }}

),

inf_cleaned_data AS (

  {{
    prophecy_basics.DataCleansing(
      relation_name = 'inf_telecom_customer_churn', 
      schema = [
        { "name": "Customer ID", "dataType": "string" }, 
        { "name": "Gender", "dataType": "string" }, 
        { "name": "Age", "dataType": "bigint" }, 
        { "name": "Married", "dataType": "string" }, 
        { "name": "Number of Dependents", "dataType": "bigint" }, 
        { "name": "City", "dataType": "string" }, 
        { "name": "Zip Code", "dataType": "bigint" }, 
        { "name": "Latitude", "dataType": "double" }, 
        { "name": "Longitude", "dataType": "double" }, 
        { "name": "Number of Referrals", "dataType": "bigint" }, 
        { "name": "Tenure in Months", "dataType": "bigint" }, 
        { "name": "Offer", "dataType": "string" }, 
        { "name": "Phone Service", "dataType": "string" }, 
        { "name": "Avg Monthly Long Distance Charges", "dataType": "double" }, 
        { "name": "Multiple Lines", "dataType": "string" }, 
        { "name": "Internet Service", "dataType": "string" }, 
        { "name": "Internet Type", "dataType": "string" }, 
        { "name": "Avg Monthly GB Download", "dataType": "bigint" }, 
        { "name": "Online Security", "dataType": "string" }, 
        { "name": "Online Backup", "dataType": "string" }, 
        { "name": "Device Protection Plan", "dataType": "string" }, 
        { "name": "Premium Tech Support", "dataType": "string" }, 
        { "name": "Streaming TV", "dataType": "string" }, 
        { "name": "Streaming Movies", "dataType": "string" }, 
        { "name": "Streaming Music", "dataType": "string" }, 
        { "name": "Unlimited Data", "dataType": "string" }, 
        { "name": "Contract", "dataType": "string" }, 
        { "name": "Paperless Billing", "dataType": "string" }, 
        { "name": "Payment Method", "dataType": "string" }, 
        { "name": "Monthly Charge", "dataType": "double" }, 
        { "name": "Total Charges", "dataType": "double" }, 
        { "name": "Total Refunds", "dataType": "double" }, 
        { "name": "Total Extra Data Charges", "dataType": "bigint" }, 
        { "name": "Total Long Distance Charges", "dataType": "double" }, 
        { "name": "Total Revenue", "dataType": "double" }, 
        { "name": "Customer Status", "dataType": "string" }, 
        { "name": "Churn Category", "dataType": "string" }, 
        { "name": "Churn Reason", "dataType": "string" }
      ], 
      modifyCase = 'none', 
      columnNames = [
        'Gender', 
        'Married', 
        'City', 
        'Offer', 
        'Phone Service', 
        'Multiple Lines', 
        'Internet Service', 
        'Internet Type', 
        'Online Security', 
        'Online Backup', 
        'Device Protection Plan', 
        'Premium Tech Support', 
        'Streaming TV', 
        'Streaming Movies', 
        'Streaming Music', 
        'Unlimited Data', 
        'Contract', 
        'Paperless Billing', 
        'Payment Method', 
        'Customer Status', 
        'Churn Category', 
        'Churn Reason'
      ], 
      replaceNullTextFields = True, 
      replaceNullTextWith = 'Unknown', 
      replaceNullForNumericFields = True, 
      replaceNullNumericWith = 0, 
      trimWhiteSpace = True, 
      removeTabsLineBreaksAndDuplicateWhitespace = True
    )
  }}

),

inf_renamed_columns AS (

  SELECT 
    `Customer ID` AS customer_id,
    `Gender` AS gender,
    `Age` AS age_numeric,
    `Married` AS married,
    `Number of Dependents` AS number_of_dependents,
    `City` AS city,
    `Zip Code` AS zip_code,
    `Latitude` AS latitude,
    `Longitude` AS longitude,
    `Number of Referrals` AS number_of_referrals,
    `Tenure in Months` AS tenure_in_months,
    `Offer` AS offer,
    `Phone Service` AS phone_service,
    `Avg Monthly Long Distance Charges` AS avg_monthly_long_distance_charges,
    `Multiple Lines` AS multiple_lines,
    `Internet Service` AS internet_service,
    `Internet Type` AS internet_type,
    `Avg Monthly GB Download` AS avg_monthly_gb_download,
    `Online Security` AS online_security,
    `Online Backup` AS online_backup,
    `Device Protection Plan` AS device_protection_plan,
    `Premium Tech Support` AS premium_tech_support,
    `Streaming TV` AS streaming_tv,
    `Streaming Movies` AS streaming_movies,
    `Streaming Music` AS streaming_music,
    `Unlimited Data` AS unlimited_data,
    `Contract` AS contract,
    `Paperless Billing` AS paperless_billing,
    `Payment Method` AS payment_method,
    `Monthly Charge` AS monthly_charge,
    `Total Charges` AS total_charges,
    `Total Refunds` AS total_refunds,
    `Total Extra Data Charges` AS total_extra_data_charges,
    `Total Long Distance Charges` AS total_long_distance_charges,
    `Total Revenue` AS total_revenue,
    `Customer Status` AS customer_status,
    `Churn Category` AS churn_category,
    `Churn Reason` AS churn_reason
  
  FROM inf_cleaned_data

),

inf_binary_features AS (

  SELECT 
    *,
    CASE
      WHEN UPPER(married) = 'YES'
        THEN 1
      ELSE 0
    END AS is_married,
    CASE
      WHEN UPPER(phone_service) = 'YES'
        THEN 1
      ELSE 0
    END AS has_phone_service,
    CASE
      WHEN UPPER(internet_service) = 'YES'
        THEN 1
      ELSE 0
    END AS has_internet_service,
    CASE
      WHEN UPPER(online_security) = 'YES'
        THEN 1
      ELSE 0
    END AS has_online_security,
    CASE
      WHEN UPPER(online_backup) = 'YES'
        THEN 1
      ELSE 0
    END AS has_online_backup,
    CASE
      WHEN UPPER(device_protection_plan) = 'YES'
        THEN 1
      ELSE 0
    END AS has_device_protection,
    CASE
      WHEN UPPER(premium_tech_support) = 'YES'
        THEN 1
      ELSE 0
    END AS has_premium_tech_support,
    CASE
      WHEN UPPER(streaming_tv) = 'YES'
        THEN 1
      ELSE 0
    END AS has_streaming_tv,
    CASE
      WHEN UPPER(streaming_movies) = 'YES'
        THEN 1
      ELSE 0
    END AS has_streaming_movies,
    CASE
      WHEN UPPER(streaming_music) = 'YES'
        THEN 1
      ELSE 0
    END AS has_streaming_music,
    CASE
      WHEN UPPER(unlimited_data) = 'YES'
        THEN 1
      ELSE 0
    END AS has_unlimited_data,
    CASE
      WHEN UPPER(paperless_billing) = 'YES'
        THEN 1
      ELSE 0
    END AS has_paperless_billing,
    CASE
      WHEN UPPER(customer_status) = 'CHURNED'
        THEN 1
      ELSE 0
    END AS churn_label
  
  FROM inf_renamed_columns

),

inf_categorical_encoding AS (

  SELECT 
    *,
    CASE
      WHEN UPPER(contract) LIKE '%MONTH-TO-MONTH%'
        THEN 0
      WHEN UPPER(contract) LIKE '%ONE YEAR%'
        THEN 1
      WHEN UPPER(contract) LIKE '%TWO YEAR%'
        THEN 2
      ELSE 0
    END AS contract_type_encoded,
    CASE
      WHEN UPPER(payment_method) LIKE '%BANK%'
        THEN 1
      WHEN UPPER(payment_method) LIKE '%CREDIT%'
        THEN 2
      WHEN UPPER(payment_method) LIKE '%MAILED%'
        THEN 0
      ELSE 0
    END AS payment_method_encoded,
    CASE
      WHEN internet_type IS NULL OR UPPER(internet_type) = 'NONE' OR UPPER(internet_type) = 'UNKNOWN'
        THEN 0
      WHEN UPPER(internet_type) LIKE '%DSL%'
        THEN 1
      WHEN UPPER(internet_type) LIKE '%FIBER%'
        THEN 2
      WHEN UPPER(internet_type) LIKE '%CABLE%'
        THEN 3
      ELSE 0
    END AS internet_type_encoded
  
  FROM inf_binary_features

),

assign_age_group AS (

  {#Categorizes customers into age groups to enhance segmentation and analysis for targeted business strategies.#}
  {{
    Hackathon_Telco_Churn.BRE_SQL_Gem(
      ['inf_categorical_encoding'], 
      [
        { "name": "customer_id", "dataType": "String" }, 
        { "name": "gender", "dataType": "String" }, 
        { "name": "age_numeric", "dataType": "Bigint" }, 
        { "name": "married", "dataType": "String" }, 
        { "name": "number_of_dependents", "dataType": "Bigint" }, 
        { "name": "city", "dataType": "String" }, 
        { "name": "zip_code", "dataType": "Bigint" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "number_of_referrals", "dataType": "Bigint" }, 
        { "name": "tenure_in_months", "dataType": "Bigint" }, 
        { "name": "offer", "dataType": "String" }, 
        { "name": "phone_service", "dataType": "String" }, 
        { "name": "avg_monthly_long_distance_charges", "dataType": "Double" }, 
        { "name": "multiple_lines", "dataType": "String" }, 
        { "name": "internet_service", "dataType": "String" }, 
        { "name": "internet_type", "dataType": "String" }, 
        { "name": "avg_monthly_gb_download", "dataType": "Bigint" }, 
        { "name": "online_security", "dataType": "String" }, 
        { "name": "online_backup", "dataType": "String" }, 
        { "name": "device_protection_plan", "dataType": "String" }, 
        { "name": "premium_tech_support", "dataType": "String" }, 
        { "name": "streaming_tv", "dataType": "String" }, 
        { "name": "streaming_movies", "dataType": "String" }, 
        { "name": "streaming_music", "dataType": "String" }, 
        { "name": "unlimited_data", "dataType": "String" }, 
        { "name": "contract", "dataType": "String" }, 
        { "name": "paperless_billing", "dataType": "String" }, 
        { "name": "payment_method", "dataType": "String" }, 
        { "name": "monthly_charge", "dataType": "Double" }, 
        { "name": "total_charges", "dataType": "Double" }, 
        { "name": "total_refunds", "dataType": "Double" }, 
        { "name": "total_extra_data_charges", "dataType": "Bigint" }, 
        { "name": "total_long_distance_charges", "dataType": "Double" }, 
        { "name": "total_revenue", "dataType": "Double" }, 
        { "name": "customer_status", "dataType": "String" }, 
        { "name": "churn_category", "dataType": "String" }, 
        { "name": "churn_reason", "dataType": "String" }, 
        { "name": "is_married", "dataType": "Integer" }, 
        { "name": "has_phone_service", "dataType": "Integer" }, 
        { "name": "has_internet_service", "dataType": "Integer" }, 
        { "name": "has_online_security", "dataType": "Integer" }, 
        { "name": "has_online_backup", "dataType": "Integer" }, 
        { "name": "has_device_protection", "dataType": "Integer" }, 
        { "name": "has_premium_tech_support", "dataType": "Integer" }, 
        { "name": "has_streaming_tv", "dataType": "Integer" }, 
        { "name": "has_streaming_movies", "dataType": "Integer" }, 
        { "name": "has_streaming_music", "dataType": "Integer" }, 
        { "name": "has_unlimited_data", "dataType": "Integer" }, 
        { "name": "has_paperless_billing", "dataType": "Integer" }, 
        { "name": "churn_label", "dataType": "Integer" }, 
        { "name": "contract_type_encoded", "dataType": "Integer" }, 
        { "name": "payment_method_encoded", "dataType": "Integer" }, 
        { "name": "internet_type_encoded", "dataType": "Integer" }
      ], 
      [
        {
          "input_column": "age_numeric", 
          "output_column": "age_group", 
          "rule_condition": "< 25", 
          "rule_output_value": "'Young'"
        }, 
        {
          "input_column": "age_numeric", 
          "output_column": "age_group", 
          "rule_condition": "<45", 
          "rule_output_value": "'Middle'"
        }, 
        {
          "input_column": "age_numeric", 
          "output_column": "age_group", 
          "rule_condition": "<65", 
          "rule_output_value": "'Senior'"
        }, 
        {
          "input_column": "age_numeric", 
          "output_column": "age_group", 
          "rule_condition": ">=65", 
          "rule_output_value": "'Elderly'"
        }
      ]
    )
  }}

),

inf_derived_features AS (

  {#Calculates customer revenue trends, service usage, refund rates, and classifies customers by how long they have stayed.#}
  SELECT 
    *,
    CASE
      WHEN tenure_in_months > 0
        THEN ROUND(total_revenue / tenure_in_months, 2)
      ELSE 0
    END AS avg_revenue_per_month,
    CASE
      WHEN total_revenue > 0
        THEN ROUND(total_charges / total_revenue, 4)
      ELSE 0
    END AS charges_to_revenue_ratio,
    CASE
      WHEN total_charges > 0
        THEN ROUND(total_refunds / total_charges, 4)
      ELSE 0
    END AS refund_rate,
    (
      has_phone_service
      + has_internet_service
      + has_online_security
      + has_online_backup
      + has_device_protection
      + has_premium_tech_support
      + has_streaming_tv
      + has_streaming_movies
      + has_streaming_music
      + has_unlimited_data
    ) AS total_services_count,
    CASE
      WHEN tenure_in_months <= 6
        THEN 'New'
      WHEN tenure_in_months <= 24
        THEN 'Growing'
      WHEN tenure_in_months <= 48
        THEN 'Established'
      ELSE 'Loyal'
    END AS tenure_category
  
  FROM inf_categorical_encoding

),

Join_1 AS (

  {#Combines detailed customer profiles with their age group classification to support customer segmentation and churn analysis.#}
  SELECT 
    in0.customer_id AS customer_id,
    in0.gender AS gender,
    in0.age_numeric AS age_numeric,
    in0.married AS married,
    in0.number_of_dependents AS number_of_dependents,
    in0.city AS city,
    in0.zip_code AS zip_code,
    in0.latitude AS latitude,
    in0.longitude AS longitude,
    in0.number_of_referrals AS number_of_referrals,
    in0.tenure_in_months AS tenure_in_months,
    in0.offer AS offer,
    in0.phone_service AS phone_service,
    in0.avg_monthly_long_distance_charges AS avg_monthly_long_distance_charges,
    in0.multiple_lines AS multiple_lines,
    in0.internet_service AS internet_service,
    in0.internet_type AS internet_type,
    in0.avg_monthly_gb_download AS avg_monthly_gb_download,
    in0.online_security AS online_security,
    in0.online_backup AS online_backup,
    in0.device_protection_plan AS device_protection_plan,
    in0.premium_tech_support AS premium_tech_support,
    in0.streaming_tv AS streaming_tv,
    in0.streaming_movies AS streaming_movies,
    in0.streaming_music AS streaming_music,
    in0.unlimited_data AS unlimited_data,
    in0.contract AS contract,
    in0.paperless_billing AS paperless_billing,
    in0.payment_method AS payment_method,
    in0.monthly_charge AS monthly_charge,
    in0.total_charges AS total_charges,
    in0.total_refunds AS total_refunds,
    in0.total_extra_data_charges AS total_extra_data_charges,
    in0.total_long_distance_charges AS total_long_distance_charges,
    in0.total_revenue AS total_revenue,
    in0.customer_status AS customer_status,
    in0.churn_category AS churn_category,
    in0.churn_reason AS churn_reason,
    in0.is_married AS is_married,
    in0.has_phone_service AS has_phone_service,
    in0.has_internet_service AS has_internet_service,
    in0.has_online_security AS has_online_security,
    in0.has_online_backup AS has_online_backup,
    in0.has_device_protection AS has_device_protection,
    in0.has_premium_tech_support AS has_premium_tech_support,
    in0.has_streaming_tv AS has_streaming_tv,
    in0.has_streaming_movies AS has_streaming_movies,
    in0.has_streaming_music AS has_streaming_music,
    in0.has_unlimited_data AS has_unlimited_data,
    in0.has_paperless_billing AS has_paperless_billing,
    in0.churn_label AS churn_label,
    in0.contract_type_encoded AS contract_type_encoded,
    in0.payment_method_encoded AS payment_method_encoded,
    in0.internet_type_encoded AS internet_type_encoded,
    in0.avg_revenue_per_month AS avg_revenue_per_month,
    in0.charges_to_revenue_ratio AS charges_to_revenue_ratio,
    in0.refund_rate AS refund_rate,
    in0.total_services_count AS total_services_count,
    in0.tenure_category AS tenure_category,
    in1.age_group AS age_group
  
  FROM inf_derived_features AS in0
  INNER JOIN assign_age_group AS in1
     ON in1.customer_id = in0.customer_id

)

SELECT *

FROM Join_1
