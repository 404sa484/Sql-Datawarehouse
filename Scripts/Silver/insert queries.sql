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

insert into silver.crm_sales_details(
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
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case
when sls_order_dt<=0  or len(sls_order_dt)!=8 then null
else cast(cast(sls_order_dt as varchar) as date) 
end as sls_order_dt,
case
when sls_ship_dt<=0  or len(sls_ship_dt)!=8 then null
else cast(cast(sls_ship_dt as varchar) as date) 
end as sls_ship_date,
case
when sls_due_dt<=0  or len(sls_due_dt)!=8 then null
else cast(cast(sls_due_dt as varchar) as date) 
end as sls_due_dt,
case
 when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
  then sls_quantity * abs(sls_price)
  else sls_sales
end as sls_sales,
case
 when sls_quantity is null or sls_quantity <= 0 or sls_quantity != sls_sales / ABS(sls_price)
  then sls_sales / ABS(sls_price)
  else sls_quantity
end as sls_quantity,
case
 when sls_price is null or sls_price <= 0 or sls_price != sls_sales / nullif(sls_quantity,1)
  then sls_sales / nullif(sls_quantity,1)
  else sls_price
end as sls_price
from bronze.crm_sales_details
