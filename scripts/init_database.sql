use master;
create database if not exists  DataWarehouse;

use DataWarehouse;

create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;



if OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_cust_info;
create table  bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

use DataWarehouse;


if OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

if OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id NVARCHAR(50),
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


if OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);


if OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);


if OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);


EXEC bronze.load_bronze;


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @Start_time DATETIME, @End_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY 
        set @batch_start_time = GETDATE();

        PRINT '========================================================';
        PRINT '======== LOAD DATA TO BRONZE LAYER =====================';
        PRINT '========================================================';

        PRINT '--------------------------------------------------------';
        PRINT '-----------LOADING CRM TABLES---------------------------';
        PRINT '--------------------------------------------------------';

        SET @Start_time = GETDATE();
        PRINT '--------Truncate table bronze.crm_cust_info-------------';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '-----------BULK INSERT into bronze.crm_cust_info--------';
        BULK INSERT bronze.crm_cust_info 
        FROM 'C:\Users\USER\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        SET @Start_time = GETDATE();
        PRINT '--------Truncate table bronze.crm_prd_info -------------';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '-----------BULK INSERT into bronze.crm_prd_info --------';
        BULK INSERT bronze.crm_prd_info 
        FROM 'C:\Users\USER\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        SET @Start_time = GETDATE();
        PRINT '--------Truncate table bronze.crm_sales_details---------';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '-----------BULK INSERT into bronze.crm_sales_details ---';
        BULK INSERT bronze.crm_sales_details 
        FROM 'C:\Users\USER\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        PRINT '========================================================';
        PRINT '======== LOAD DATA TO BRONZE LAYER FROM ERP============='; 
        PRINT '========================================================';

        SET @Start_time = GETDATE();
        PRINT '--------Truncate table bronze.erp_cust_az12 ------------';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '-----------BULK INSERT into bronze.erp_cust_az12 -------';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\USER\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        SET @Start_time = GETDATE();
        PRINT '--------Truncate table bronze.erp_loc_a101 -------------';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '-----------BULK INSERT into bronze.erp_loc_a101--------';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\USER\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        SET @Start_time = GETDATE();
        PRINT '--------Truncate table bronze.erp_px_cat_g1v2 ----------';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '-----------BULK INSERT into bronze.erp_px_cat_g1v2------';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\USER\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

		set @batch_end_time = GETDATE();
		PRINT '>>Total load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==============================';

    END TRY
    BEGIN CATCH
        PRINT '=========================================================';
        PRINT '>>> ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT '>>> ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT '=========================================================';
    END CATCH
END;



