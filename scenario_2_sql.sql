--- SQL to create the table to demonstrate more in-depth ingestion using `PIPE`, `TASK`, and `STREAM` . This table will house VARIANT data from a JSON.
CREATE OR REPLACE TABLE PAYMENTS_JSON ( 
	payment_data VARIANT, 
	creationDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
); 


--- QL to create the table used for JOINs to demo overall functionality. This will act as the primary source table, and will be joined with the PAYMENTS_JSON table.
CREATE OR REPLACE TABLE INVOICES ( 
	id number autoincrement start 1 increment 1 PRIMARY KEY, 
	customerID INT, 
	orderId INT, 
	totalCost NUMBER, 
	creationDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
); 


--- SQL to create the target table used to demo the `PIPE`, `TASK`, and `STREAM` pipeline. The transformed data will go into this table via a MERGE...INSERT...UPDATE
CREATE OR REPLACE TABLE INVOICE_TRACKING (
	id number autoincrement start 1 increment 1 PRIMARY KEY, 
	invoiceId INT, 
	customerId INT, 
	totalCost NUMBER, 
	totalPaid NUMBER, 
	paidOff BOOLEAN, 
	lastPayment NUMBER, 
	lastPaymentDate TIMESTAMP 
);


--- SQL to create the File Format
CREATE OR REPLACE FILE FORMAT my_json_format 
	TYPE = JSON;


--- SQL to create the Storage Integration, must use role `ACCOUNTADMIN`. The `GRANT` should be tailored to whatever role you plan to use.
CREATE OR REPLACE STORAGE INTEGRATION SNOWFLAKE_DATA_PIPELINE_STORAGE_INTEGRATION 
	TYPE = EXTERNAL_STAGE 
	STORAGE_PROVIDER = 'GCS' 
	ENABLED = TRUE 
	STORAGE_ALLOWED_LOCATIONS = ('gcs://<your bucket url here>'); 

GRANT USAGE ON INTEGRATION SNOWFLAKE_DATA_PIPELINE_STORAGE_INTEGRATION TO ROLE SYSADMIN;


--- SQL to create the stage linked to the bucket, switch back to your intended role. 
CREATE OR REPLACE STAGE SNOWFLAKE_DATA_PIPELINE 
	URL='gcs://<your bucket url here>' 
	STORAGE_INTEGRATION = SNOWFLAKE_DATA_PIPELINE_STORAGE_INTEGRATION;


--- SQL to create the Integration to notify the Snowpipe when new files land in the bucket, use role `ACCOUNTADMIN` . The GRANT should be tailored to whatever role you plan to use.
CREATE OR REPLACE NOTIFICATION INTEGRATION SNOWFLAKE_DATA_PIPELINE_NOTIFICATION 
	TYPE = QUEUE 
	NOTIFICATION_PROVIDER = GCP_PUBSUB
	ENABLED = true
	GCP_PUBSUB_SUBSCRIPTION_NAME = '<subscription_id>'; 

GRANT USAGE ON INTEGRATION SNOWFLAKE_DATA_PIPELINE_NOTIFICATION TO ROLE SYSADMIN;


--- SQL to create the Snowpipe to ingest the CSV files,  switch back to your intended role. 
CREATE OR REPLACE PIPE SNOWFLAKE_DATA_PIPELINE_PIPE_JSON
  AUTO_INGEST = true
  INTEGRATION = 'SNOWFLAKE_DATA_PIPELINE_NOTIFICATION'
  AS
COPY INTO PAYMENTS_JSON (PAYMENT_DATA, CREATIONDATE)
  FROM (
    select $1, CURRENT_TIMESTAMP()
    from @SNOWFLAKE_DATA_PIPELINE
  )
  FILE_FORMAT = (FORMAT_NAME = 'my_json_format')
  PATTERN = '.*[.]json'
  ON_ERROR = SKIP_FILE;


--- SQL to create the  `STREAM` to catch changes in the staging table where the JSON files land
CREATE STREAM SNOWFLAKE_DATA_PIPELINE_PIPE_STREAM ON TABLE DATA_PIPELINE.TEST.PAYMENTS_JSON;


--- SQL to create the `TASK` that will be triggered when the stream logs changes
CREATE OR REPLACE TASK SNOWFLAKE_DATA_PIPELINE_PIPE_TASK
    WAREHOUSE = compute_wh
    SCHEDULE = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('SNOWFLAKE_DATA_PIPELINE_PIPE_STREAM') 
AS
    BEGIN
	    -- Merging the staging table and invoice table into the INVOIVE_TRACKING table
        MERGE INTO INVOICE_TRACKING
            USING (
                with initial_payments as (
                    SELECT
                        r.value:invoiceId AS invoiceId,
                        r.value:amountPaid::NUMBER AS amountPaid,
                        TO_TIMESTAMP(r.value:paymentDate::STRING, 'YYYY-MM-DDTHH24:MI:SS') AS paymentDate,
                        SUM(r.value:amountPaid::NUMBER) OVER (PARTITION BY r.value:invoiceId) AS totalPaid,
                        ROW_NUMBER() OVER (PARTITION BY r.value:invoiceId ORDER BY TO_TIMESTAMP(r.value:paymentDate::STRING, 'YYYY-MM-DDTHH24:MI:SS') DESC) AS rn
                    FROM
                        PAYMENTS_JSON t,
                        LATERAL FLATTEN(input => t.payment_data:payment_data) r
                ),
                final_payments as (
                    SELECT 
                        *
                    FROM initial_payments
                    WHERE
                        rn = 1
                ),
                final_invoices as (
                    SELECT 
                        id as invoiceId,
                        customerId,
                        orderId,
                        totalCost
                    from INVOICES
                )
                SELECT 
                    i.invoiceId as invoiceId,
                    i.customerId as customerId,
                    i.totalCost as totalCost,
                    p.totalPaid as totalPaid,
                    CASE
                        WHEN p.totalPaid >= i.totalCost THEN true
                        ELSE false
                    END as paidOff,
                    p.amountPaid as lastPayment,
                    p.paymentDate as lastPaymentDate
                FROM final_invoices i
                LEFT JOIN final_payments p ON i.invoiceId = p.invoiceId
            ) as cte
        ON cte.invoiceId = INVOICE_TRACKING.invoiceId
        WHEN MATCHED THEN
          UPDATE SET 
            INVOICE_TRACKING.customerId = cte.customerId,
            INVOICE_TRACKING.totalCost = cte.totalCost,
            INVOICE_TRACKING.totalPaid = cte.totalPaid,
            INVOICE_TRACKING.paidOff = cte.paidOff,
            INVOICE_TRACKING.lastPayment = cte.lastPayment,
            INVOICE_TRACKING.lastPaymentDate = cte.lastPaymentDate
        WHEN NOT MATCHED THEN
          INSERT (invoiceId, customerId, totalCost, totalPaid, paidOff, lastPayment, lastPaymentDate)
            VALUES (cte.invoiceId, cte.customerId, cte.totalCost, cte.totalPaid, cte.paidOff, cte.lastPayment, cte.lastPaymentDate);
            
		-- Truncate the staging table to remove already processed data
        TRUNCATE PAYMENTS_JSON;

		-- Reset the stream so the TRUNCATE does not trigger another run
        CREATE OR REPLACE TEMP TABLE RESET_TBL AS
            SELECT * FROM SNOWFLAKE_DATA_PIPELINE_PIPE_STREAM;
    END;


--- SQL to enable the TASK, as a `TASK` is disabled by default upon creation
ALTER TASK SNOWFLAKE_DATA_PIPELINE_PIPE_TASK RESUME;
