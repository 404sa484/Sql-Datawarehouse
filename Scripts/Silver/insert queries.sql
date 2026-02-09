---insert cust_info (clean data) to silver cust_info table

insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case
when upper(trim(cst_marital_status))='S' then 'Single'
 when upper(trim(cst_marital_status))='M' then 'Married'
else 'n/a'
end as cst_marital_status,
case
when upper(trim(cst_gndr))='M' then 'male'
when upper(trim(cst_gndr))='F' then 'Female'
else 'n/a'
end as cst_gndr,
cst_create_date
from
(
select*,
row_number() over(partition by cst_id order by cst_create_date desc) as cust_rank_by_date
from bronze.crm_cust_info
) as t
where cust_rank_by_date=1 and cst_id is not null;



---insert bronze.crm.prd_info (clean data) to silver product_info table

insert into silver.crm_prd_info(
   prd_id ,
   cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
prd_id,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7, len(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
case
 when upper(trim(prd_line)) = 'M' then 'Mountain'
 when upper(trim(prd_line)) = 'S' then 'Other Sales'
 when upper(trim(prd_line)) = 'T' then 'Touring'
 when upper(trim(prd_line)) = 'R' then 'Road'
else 'n/a'
end as prd_line,
CAST(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
from
bronze.crm_prd_info;



----insert bronze.sales_details (clean data) into silver.Sales_details

INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;



----insert bronze.erp_custaz12(clean data) insert into silver.erp_cust_az12

insert into silver.erp_cust_az12(
cid,
bdate,
gen
)
select
case
when cid like 'NAS%' then substring(cid,4,len(cid))
else cid
end as cid,
case
when bdate>getdate() then Null
else bdate
end as bdate,
case
when upper(trim(gen)) in ('F','Female') then 'Female'
when upper(trim(gen)) in ('M','Male') then 'male'
else 'n/a'
end as gen
from bronze.erp_cust_Az12


----insert bronze.erp_loc_a101(clean data) insert into sliver.erp_loc_a101
insert into silver.erp_loc_a101(
cid,
cntry
)
select
replace(cid,'-','') as cid,
case
when upper(trim(cntry)) in ('US','USA','Unitied states') then 'Unitied States'
when upper(trim(cntry)) in('DE','Germany') then 'Germany'
when trim(cntry) =' ' or cntry is null then 'n/a'
else trim(cntry) 
end as cntry
from bronze.erp_Loc_A101



----insert bronze.erp_px_cat_g1v2

insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
select* from bronze.erp_px_catat_g1v2



