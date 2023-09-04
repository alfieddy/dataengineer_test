use database raw;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxx:role/deshowcasesnowflakerole'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://de-showcase-001/landing/')
  ;
  
DESC INTEGRATION s3_int
;
--  Create Stage
CREATE OR REPLACE STAGE my_s3_stage
storage_integration = s3_int
url = 's3://de-showcase-001/landing/'
;

describe stage my_s3_stage
;

use database raw
;
use schema public
;
list @my_s3_stage
;

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
REPLACE_INVALID_CHARACTERS = TRUE 
;

create or replace table raw.public.nmi_info
(
filename varchar,
insert_datetime varchar,
nmi varchar,
state varchar,
interval varchar
)
;

create or replace table raw.public.error_nmi_info
(
nmi varchar,
state varchar,
interval varchar
)
;

copy into raw.public.nmi_info
from (SELECT METADATA$FILENAME,
             current_timestamp, 
             $1,$2,$3
         FROM @my_s3_stage)
PATTERN='.*Reference.*.csv'
file_format = my_csv_format
-- on_error='continue'
;

-- Log Errors in Error Handling Table
-- Load errors in files into error log table
-- Fix any issues with the data files before attempting to load the data again 
CREATE OR REPLACE TABLE error_log (FILE VARCHAR, ROW_NUMBER INTEGER, ERROR VARCHAR, ERROR_COLUMN VARCHAR);
;
COPY INTO raw.public.error_nmi_info 
FROM @my_s3_stage
PATTERN='.*Reference.*.csv'
VALIDATION_MODE = 'RETURN_ERRORS' 
;

INSERT INTO error_log 
SELECT file,row_number,error,column_name  
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

select * from error_log;


create or replace table raw.public.nmi
(
filename varchar,
insert_datetime varchar,
AESTTime varchar,
Quantity varchar,
Unit varchar
)
;

copy into raw.public.nmi
FROM (SELECT METADATA$FILENAME,
             current_timestamp, 
             $1,$2,$3
         FROM @my_s3_stage)
PATTERN='.*NMI.*.csv'
file_format = my_csv_format
on_error='continue'
;


SELECT *
  FROM information_schema.load_history
  WHERE schema_name=current_schema() 
  AND table_name in ('nmi_info', 'nmi')
;

use database transform
;

/* Transform from Landing to Staging */
create or replace table transform.public.staging_nmi as
select filename,
       insert_datetime,
       substr(right(filename, 9), 1,5) as nmi,
       try_to_date(aesttime::VARCHAR, 'YYYY-MM-DD HH24:MI:SS') as date,
       TRY_TO_TIMESTAMP(aesttime::VARCHAR, 'YYYY-MM-DD HH24:MI:SS') as nim_date_ts,
       TRY_TO_NUMeric(quantity, 10, 2) AS quantity,
       unit,
       current_timestamp as last_updated
  from raw.public.nmi
  order by nmi, nim_date_ts
;  

create or replace table transform.public.staging_nmi_info as
select * from  
raw.public.nmi_info
;

-- 
create or replace table transform.public.nmi_info as  
select md5(nmi) as dw_nmi_key,
       nmi, 
       state, 
       interval,
       CASE state WHEN 'QLD' THEN 'Australia/Brisbane'
                  WHEN 'VIC' THEN 'Australia/Melbourne'
                  WHEN 'NSW' THEN 'Australia/Sydney'
                  WHEN 'WA' THEN 'Australia/Perth'
        END AS timezone          
  from public.staging_nmi_info 
 where nmi is not null
 order by  1;

use database analytics;

create or replace table analytics.public.nmi_dim as  
select * 
  from transform.public.nmi_info
  ;

 -- select aesttime, quantity,unit from public.nmi;
create or replace table analytics.public.nmi_daily_hourly_reading_fact as
with transform as (
select to_date(convert_timezone('Australia/Melbourne', info.timezone, nmi.nim_date_ts::timestamp_ntz)) as date,
       hour(convert_timezone('Australia/Melbourne', info.timezone, nmi.nim_date_ts::timestamp_ntz)) as hour, -- convert to local timezone
       info.state,
       nmi.nmi,
       nmi.unit,
       round(avg(quantity),2) as avg_quantity,
       min(quantity) as min_quantity,
       max(quantity) as max_quantity,
       round(avg(quantity) * 60,2) as approx_hourly_quantity,
       count(nmi.nmi) as nmi_reading_count
  from transform.public.staging_nmi nmi
  join transform.public.nmi_info info on nmi.nmi = info.nmi
 where quantity is not null -- remove bad data until reload
   and nmi.nim_date_ts is not null -- remove bad data until reload
 group by all
  order by 1,2,3,4,5
  ),
  agg_transform as 
  (select md5(nmi) as dw_nmi_key,
          md5(hour) as dw_hour_key,
          md5(state) as dw_state_key,
         *,
         round(lag(avg_quantity) over (PARTITION BY nmi, date, unit order by nmi,date,hour,unit),2) as avg_quantity_prev_hour,
         round(avg_quantity - lag(avg_quantity) over (PARTITION BY nmi, date, unit order by nmi,date,unit),2) as increase_over_prev_hour, 
         round(div0((avg_quantity - avg_quantity_prev_hour), avg_quantity),2) as percent_change_over_prev_hour
    from transform
    order by nmi, date, unit,hour
    )
  select *,
         max(percent_change_over_prev_hour) over (PARTITION BY nmi, date, unit order by nmi, date, unit) as max_quantity_prev_hour,
         min(percent_change_over_prev_hour) over (PARTITION BY nmi, date, unit order by nmi, date, unit) as min_quantity_prev_hour,
         case when max_quantity_prev_hour = percent_change_over_prev_hour then hour end as max_hour_percent_change,
         case when min_quantity_prev_hour = percent_change_over_prev_hour then hour end as min_hour_percent_change
    from  agg_transform
  ;

-- Create Date Dim
create or replace table analytics.public.date_dim as 
WITH date_range AS (
  SELECT '2017-10-01'::DATE AS start_date,
         2018-09-30 AS end_date
),
date_list AS (
  SELECT DATEADD(DAY, seq4(), start_date) AS date
  FROM date_range,
       TABLE(GENERATOR(rowcount => 730))),
date_parts AS (
  SELECT date,
         YEAR(date) AS year,
         QUARTER(date) AS quarter,
         MONTH(date) AS month,
         DAYOFMONTH(date) AS day_of_month,
         monthname(date) as day_of_month_name,
         DAYOFWEEK(date) AS day_of_week,
         DAYNAME(date) AS day_of_week_name,
         WEEKOFYEAR(date) AS week_of_year
  FROM date_list
)
SELECT * 
FROM date_parts
;

-- Create a table to store the hour dimension
CREATE OR REPLACE TABLE analytics.public.hour_dim (
  dw_hour_key varchar(32),
  hour_id INT PRIMARY KEY,  
  hour_name VARCHAR(10),  
  hour_12 VARCHAR(10),  
  am_pm VARCHAR(2)
);

-- Populate the table with data using a sequence generator
INSERT INTO analytics.public.hour_dim
SELECT 
  MD5(SEQ4()) AS DW_HOUR_ID,
  SEQ4() AS hour_id,
  LPAD(hour_id::VARCHAR,2,'0') || ':00' AS hour_name,
  CASE WHEN hour_id = 0 THEN '12:00 AM'
       WHEN hour_id < 12 THEN LPAD(hour_id::VARCHAR,2,'0') || ':00 AM'
       WHEN hour_id = 12 THEN '12:00 PM'
       ELSE LPAD((hour_id - 12)::VARCHAR,2,'0') || ':00 PM' END AS hour_12,
  CASE WHEN hour_id < 12 THEN 'AM' ELSE 'PM' END AS am_pm
FROM TABLE(GENERATOR(ROWCOUNT => 24)) -- Generate 24 rows of data
ORDER BY hour_id; -- Order by the hour value

-- Verify the result
SELECT * FROM analytics.public.hour_dim;


----------- FINAL TRANSFORMED TABLES ---------
  
select * from analytics.public.nmi_daily_hourly_reading_fact where dw_hour_key = 'cfcd208495d565ef66e7dff9f98764da' limit 100;
select * from analytics.public.nmi_dim;
select * from analytics.public.date_dim;
SELECT * FROM analytics.public.hour_dim;

----------- FINAL TRANSFORMED TABLES ---------

