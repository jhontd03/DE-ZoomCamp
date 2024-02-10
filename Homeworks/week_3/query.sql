-- Create External Table

CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_green_tripdata`
OPTIONS (
format = 'PARQUET',
  uris = ['gs://taxi_green_2022/trip-data/green_tripdata_2022-*.parquet']
);

-- Create internal or materialized table

CREATE OR REPLACE TABLE ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM ny_taxi.external_green_tripdata;

-- Question 1: What is count of records for the 2022 Green Taxi Data

SELECT 
 COUNT(1) 
FROM
 ny_taxi.external_green_tripdata

-- Question 2

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM (
  SELECT PULocationID
  --FROM ny_taxi.external_green_tripdata
  FROM ny_taxi.green_tripdata_non_partitoned  
  UNION ALL
  SELECT PULocationID
  --FROM ny_taxi.external_green_tripdata
  FROM ny_taxi.green_tripdata_non_partitoned
);

-- Question 3 - How many records have a fare_amount of 0?

SELECT 
  COUNT(*) AS num_records_fare_amount_zero
FROM 
  ny_taxi.external_green_tripdata
WHERE fare_amount = 0;

-- Question 4

CREATE TABLE `ny_taxi.optimized_table`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `ny_taxi.green_tripdata_non_partitoned`;

-- Question 5

SELECT DISTINCT PULocationID
FROM `ny_taxi.green_tripdata_non_partitoned`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT PULocationID
FROM `ny_taxi.optimized_table`
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
