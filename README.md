# module3

CREATE OR REPLACE EXTERNAL TABLE taxi_datset.my_external_table
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ex3_bucket/*.parquet']
);

CREATE OR REPLACE TABLE taxi_datset.regular_table AS
SELECT * FROM taxi_datset.my_external_table;

CREATE OR REPLACE TABLE taxi_datset.partitional_table
PARTITION BY DATE(tpep_dropoff_datetime) AS
SELECT * FROM taxi_datset.my_external_table;


SELECT * FROM taxi_datset.regular_table LIMIT 10;

SELECT COUNT(*) FROM taxi_datset.regular_table WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024;

SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_count FROM taxi_datset.my_external_table;
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_count FROM taxi_datset.regular_table;

SELECT PULocationID FROM taxi_datset.regular_table;
SELECT PULocationID, DOLocationID FROM taxi_datset.regular_table;

SELECT fare_amount FROM taxi_datset.regular_table;
SELECT COUNT(1) FROM taxi_datset.regular_table WHERE fare_amount=0;


SELECT DISTINCT VendorID
FROM taxi_datset.regular_table
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM taxi_datset.partitional_table
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

