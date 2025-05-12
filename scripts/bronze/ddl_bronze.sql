EXEC bronze.load_bronze;

Go
  
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

Go



