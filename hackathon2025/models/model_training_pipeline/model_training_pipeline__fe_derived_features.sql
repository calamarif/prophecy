{{
  config({    
    "materialized": "ephemeral",
    "database": "westpac",
    "schema": "callum"
  })
}}

WITH telecom_customer_churn AS (

  SELECT * 
  
  FROM {{ source('westpac_callum', 'telecom_customer_churn') }}

),

dc_cleaned_data AS (

  {{
    prophecy_basics.DataCleansing(
      relation_name = 'telecom_customer_churn', 
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

fe_renamed_columns AS (

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
  
  FROM dc_cleaned_data AS fe_cleaned_data

),

fe_binary_features AS (

  SELECT 
    *,
    -- Binary encoding for Yes/No fields
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
    -- Churn label (target variable)
    CASE
      WHEN UPPER(customer_status) = 'CHURNED'
        THEN 1
      ELSE 0
    END AS churn_label
  
  FROM fe_renamed_columns

),

fe_categorical_encoding AS (

  SELECT 
    *,
    -- Contract type encoding (higher = longer commitment)
    CASE
      WHEN UPPER(contract) LIKE '%MONTH-TO-MONTH%'
        THEN 0
      WHEN UPPER(contract) LIKE '%ONE YEAR%'
        THEN 1
      WHEN UPPER(contract) LIKE '%TWO YEAR%'
        THEN 2
      ELSE 0
    END AS contract_type_encoded,
    -- Payment method encoding
    CASE
      WHEN UPPER(payment_method) LIKE '%BANK%'
        THEN 1
      WHEN UPPER(payment_method) LIKE '%CREDIT%'
        THEN 2
      WHEN UPPER(payment_method) LIKE '%MAILED%'
        THEN 0
      ELSE 0
    END AS payment_method_encoded,
    -- Internet type encoding
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
  
  FROM fe_binary_features

),

fe_derived_features AS (

  SELECT 
    *,
    -- Revenue per month (average spend)
    CASE
      WHEN tenure_in_months > 0
        THEN ROUND(total_revenue / tenure_in_months, 2)
      ELSE 0
    END AS avg_revenue_per_month,
    -- Charges to revenue ratio
    CASE
      WHEN total_revenue > 0
        THEN ROUND(total_charges / total_revenue, 4)
      ELSE 0
    END AS charges_to_revenue_ratio,
    -- Refund rate
    CASE
      WHEN total_charges > 0
        THEN ROUND(total_refunds / total_charges, 4)
      ELSE 0
    END AS refund_rate,
    -- Service count (number of services used)
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
    -- Customer tenure category
    CASE
      WHEN tenure_in_months <= 6
        THEN 'New'
      WHEN tenure_in_months <= 24
        THEN 'Growing'
      WHEN tenure_in_months <= 48
        THEN 'Established'
      ELSE 'Loyal'
    END AS tenure_category,
    -- Age group
    CASE
      WHEN age_numeric < 25
        THEN 'Young'
      WHEN age_numeric < 45
        THEN 'Middle'
      WHEN age_numeric < 65
        THEN 'Senior'
      ELSE 'Elderly'
    END AS age_group
  
  FROM fe_categorical_encoding

)

SELECT *

FROM fe_derived_features
