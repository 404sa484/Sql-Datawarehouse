-- Checking for unwanted spaces in order num

select * from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

select * from bronze.crm_sales_details
where sls_ord_num is null

-- check if any product keys are there in sales details which are not in 
-- crm_prd_info table

select * from bronze.crm_sales_details
where sls_prd_key not in ( select prd_key from silver.crm_prd_info)

-- check if any cust id are there in sales details which are not in 
-- crm_cst_info table

select * from bronze.crm_sales_details
where sls_cust_id not in ( select cst_id from silver.crm_cust_info)

-- Check for invalid dates like null , <= 0 and length must be 8


select 
sls_order_dt
from bronze.crm_sales_details
where sls_order_dt is null or sls_order_dt <= 0  or len(sls_order_dt) != 8;


-- sales, qty and price

-- sales - qty * price
-- price - sales / Q
-- Qty - sales / price

-- sls_sales is null or sls_sales != sls_quantity * sls_price

-- sls_quantity is null or sls_quantity != sls_sales / sls_price  or sls_quantity = 0
select sls_sales,sls_quantity,sls_price from bronze.crm_sales_details
where sls_price is null or sls_price != sls_sales / sls_quantity or sls_price <= 0 ;
