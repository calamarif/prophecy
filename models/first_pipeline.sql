WITH l0_bronze_customer_snapshots AS (

  SELECT * 
  
  FROM {{ source('analyst_samples.retail', 'l0_bronze_customer_snapshots') }}

),

customer_full_details AS (

  {#Provides a detailed customer profile including contact information, address, and separates first and last names for easier identification.#}
  SELECT 
    customer_name AS customer_name,
    customer_id AS customer_id,
    signup_date AS signup_date,
    phone_number AS phone_number,
    street_number AS street_number,
    street_name AS street_name,
    unit AS unit,
    city AS city,
    state AS state,
    postcode AS postcode,
    lat AS lat,
    lon AS lon,
    SPLIT(customer_name, ' ')[0] AS first_name,
    SPLIT(customer_name, ' ')[SIZE(SPLIT(customer_name, ' ')) - 1] AS last_name
  
  FROM l0_bronze_customer_snapshots AS in0

)

SELECT *

FROM customer_full_details
