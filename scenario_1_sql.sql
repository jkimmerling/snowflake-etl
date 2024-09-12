---SQL to create the table
TABLE PAYMENTS_CSV ( 
	id number autoincrement start 1 increment 1 PRIMARY KEY, 
	invoiceId INT, 
	amountPaid NUMBER, 
	paymentDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);


---SQL to create the File Format
CREATE OR REPLACE FILE FORMAT my_csv_format 
	TYPE = CSV FIELD_DELIMITER = ',' 
	FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
	NULL_IF = ('NULL', 'null','') 
	ESCAPE_UNENCLOSED_FIELD = None 
	EMPTY_FIELD_AS_NULL = true 
	skip_header = 1 
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


---SQL to create the Storage Integration, must use role `ACCOUNTADMIN`. The `GRANT` should be tailored to whatever role you plan to use.
CREATE OR REPLACE STORAGE INTEGRATION SNOWFLAKE_DATA_PIPELINE_STORAGE_INTEGRATION 
	TYPE = EXTERNAL_STAGE 
	STORAGE_PROVIDER = 'GCS' 
	ENABLED = TRUE 
	STORAGE_ALLOWED_LOCATIONS = ('gcs://<your bucket url here>'); 

GRANT USAGE ON INTEGRATION SNOWFLAKE_DATA_PIPELINE_STORAGE_INTEGRATION TO ROLE SYSADMIN;


---SQL to create the stage linked to the bucket, switch back to your intended role. 
CREATE OR REPLACE STAGE SNOWFLAKE_DATA_PIPELINE 
	URL='gcs://<your bucket url here>' 
	STORAGE_INTEGRATION = SNOWFLAKE_DATA_PIPELINE_STORAGE_INTEGRATION;


---SQL to create the Integration to notify the Snowpipe when new files land in the bucket, use role `ACCOUNTADMIN` . The GRANT should be tailored to whatever role you plan to use.
CREATE OR REPLACE NOTIFICATION INTEGRATION SNOWFLAKE_DATA_PIPELINE_NOTIFICATION 
	TYPE = QUEUE 
	NOTIFICATION_PROVIDER = GCP_PUBSUB
	ENABLED = true
	GCP_PUBSUB_SUBSCRIPTION_NAME = '<subscription_id>'; 

GRANT USAGE ON INTEGRATION SNOWFLAKE_DATA_PIPELINE_NOTIFICATION TO ROLE SYSADMIN;


---SQL to create the Snowpipe to ingest the CSV files,  switch back to your intended role. 
CREATE OR REPLACE PIPE SNOWFLAKE_DATA_PIPELINE_PIPE_CSV 
	AUTO_INGEST = true 
	INTEGRATION = 'SNOWFLAKE_DATA_PIPELINE_NOTIFICATION' 
AS 
	COPY INTO DATA_PIPELINE.TEST.PAYMENTS_CSV (invoiceId, amountPaid, paymentDate) 
		FROM ( select $1, $2, $3 from @SNOWFLAKE_DATA_PIPELINE ) 
		FILE_FORMAT = (FORMAT_NAME = 'my_csv_format') 
		PATTERN = '.*[.]csv' 
		ON_ERROR = SKIP_FILE;
