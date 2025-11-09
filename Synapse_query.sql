SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://olistdatastorageaccntriz.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1

CREATE SCHEMA gold

CREATE VIEW gold.final
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://olistdatastorageaccntriz.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1

SELECT * FROM gold.final

CREATE VIEW gold.final2
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://olistdatastorageaccntriz.dfs.core.windows.net/olistdata/silver/',
        FORMAT = 'PARQUET'
    ) AS result2
WHERE order_status = 'delivered'


-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = '-------';
-- CREATE DATABASE SCOPED CREDENTIAL rizhwanadmin WITH IDENTITY = 'Managed Identity';

-- SELECT * from sys.database_credentials

CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://olistdatastorageaccntriz.dfs.core.windows.net/olistdata/gold',
    CREDENTIAL = rizhwanadmin
);

CREATE EXTERNAL TABLE gold.finalTable WITH (
        LOCATION = 'finalServing',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final2;
