use DataWarehouse;

GO
EXEC silver.load_silver;

GO 

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

DECLARE @Start_time DATETIME, @End_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY 
        set @batch_start_time = GETDATE();
        PRINT '========================================================';
        PRINT '======== LOAD DATA TO SILVER LAYER =====================';
        PRINT '========================================================';

        PRINT '--------------------------------------------------------';
        PRINT '-----------LOADING CRM TABLES---------------------------';
        PRINT '--------------------------------------------------------';
SET @Start_time = GETDATE();
PRINT '--------Truncate table silver.crm_cust_info-------------';

TRUNCATE TABLE silver.crm_cust_info;

PRINT '-----------INSERT into silver.crm_cust_info--------';

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE  
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
	where cst_id is not null
) rnk
WHERE flag_last = 1;
SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

SET @Start_time = GETDATE();

PRINT '--------Truncate table silver.crm_prd_info -------------';

TRUNCATE TABLE silver.crm_prd_info;

PRINT '----------- INSERT into silver.crm_prd_info --------';

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
	prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select 
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	 ELSE 'n/a'
END prd_line,
CAST(prd_start_dt AS DATE) as prd_start_dt,
CAST(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as DATE) as prd_end_dt
from bronze.crm_prd_info;

SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

        SET @Start_time = GETDATE();


PRINT '--------Truncate table silver.crm_sales_details---------';
 
TRUNCATE TABLE silver.crm_sales_details;

PRINT '-----------INSERT into silver.crm_sales_details ---';

INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity,
sls_price 
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN TRY_CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) IS NULL THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
    END AS sls_order_dt,
	CASE 
        WHEN TRY_CAST(CAST(  sls_ship_dt AS VARCHAR(8)) AS DATE) IS NULL THEN NULL
        ELSE CAST(CAST(  sls_ship_dt AS VARCHAR(8)) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN TRY_CAST(CAST(  sls_due_dt AS VARCHAR(8)) AS DATE) IS NULL THEN NULL
        ELSE CAST(CAST(  sls_due_dt AS VARCHAR(8)) AS DATE)
    END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE  sls_sales
	END as sls_sales ,
	sls_quantity,
	CASE WHEN sls_price IS NULL or sls_price <= 0 
		 THEN sls_sales /  NULLIF( sls_quantity,0)
		 ELSE sls_price
	END as sls_price
FROM bronze.crm_sales_details;

 SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';


PRINT '========================================================';
PRINT '======== LOAD DATA TO SILVER LAYER FROM ERP============='; 
PRINT '========================================================';

 SET @Start_time = GETDATE();
PRINT '--------Truncate table silver.erp_cust_az12 ------------';

TRUNCATE TABLE silver.erp_cust_az12;

PRINT '-----------INSERT into silver.erp_cust_az12 -------';


INSERT into silver.erp_cust_az12(
cid,
bdate,
gen
)
select
CASE WHEN cid LIKE 'NAS%' THEN substring(cid,4,len(cid))
     ELSE cid
END as cid,
CASE WHEN bdate > GETDATE() THEN NUll
     ELSE bdate
END as bdate ,
CASE WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
     WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
	 ELSE 'n/a'
END as gen
from bronze.erp_cust_az12;

SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

SET @Start_time = GETDATE();
PRINT '--------Truncate table silver.erp_loc_a101 -------------';

TRUNCATE TABLE silver.erp_loc_a101;

PRINT '-----------INSERT into silver.erp_loc_a101--------';

INSERT INTO silver.erp_loc_a101(cid,cntry)
select 
Replace(cid,'-','') as cid ,
CASE WHEN UPPER(TRIM(cntry)) in ('US','USA') THEN 'United States'	
	 WHEN TRIM(cntry) =' ' OR cntry is null THEN 'n/a'
	 WHEN UPPER(TRIM(cntry))= 'DE' THEN 'Germany'
     ELSE TRIM(cntry)
END as cntry
from bronze.erp_loc_a101;

SET @End_time = GETDATE();
        PRINT '>>load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------';

SET @Start_time = GETDATE();

PRINT '--------Truncate table silver.erp_px_cat_g1v2 ----------';
 
TRUNCATE TABLE silver.erp_px_cat_g1v2;

PRINT '-----------INSERT into silver.erp_px_cat_g1v2------';

insert into silver.erp_px_cat_g1v2(id ,cat,subcat,maintenance)
select 
TRIM(id) as id ,
TRIM(cat) as cat,
TRIM(subcat) as subcat,
TRIM(maintenance) as maintenance
from bronze.erp_px_cat_g1v2;
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
