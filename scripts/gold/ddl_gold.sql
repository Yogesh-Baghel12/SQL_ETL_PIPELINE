USE datewarehouse;
GO 
  
if OBJECTS_ID('gold.dim_customers','V') is not null
DROP VIEW gold.dim_customers;

GO

create VIEW gold.dim_customers as 
select 
	row_number() over(order by cst_id) as customer_key, 
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	     ELSE coalesce(ca.gen,'n/a')
	END as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on		ci.cst_key =  ca.cid
LEFT JOIN silver.erp_loc_a101 la
on		ci.cst_key = la.cid;


GO 
  
if OBJECT_ID('gold.dim_products','V') is not null
DROP VIEW gold.dim_products;

GO
  
create VIEW gold.dim_products as
select 
row_number() over (order by p.prd_start_dt,p.prd_key) as product_key, 
p.prd_id as product_id,
p.prd_key as product_number,
p.prd_nm as product_name,
p.cat_id as category_id,
pa.cat as category,
pa.subcat as subcategory,
pa.maintenance,
p.prd_cost as cost,
p.prd_line as product_line,
cast(cast(p.prd_start_dt as varchar) as Date ) as start_date
from silver.crm_prd_info as p
LEFT JOIN silver.erp_px_cat_g1v2 as pa
on p.cat_id = pa.id
where p.prd_end_dt is NULL;

GO

if OBJECT_ID('gold.fact_sales','V') is not null
 DROP VIEW gold.fact_sales;

GO
  
create VIEW gold.fact_sales as
select 
sa.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sa.sls_order_dt as order_date,
sa.sls_ship_dt as shipping_date,
sa.sls_due_dt as due_date,
sa.sls_sales as sales_amount,
sa.sls_quantity as qunatity,
sa.sls_price as price
from silver.crm_sales_details as sa
LEFT JOIN gold.dim_products as pr
on sa.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
on sa.sls_cust_id = cu.customer_id



