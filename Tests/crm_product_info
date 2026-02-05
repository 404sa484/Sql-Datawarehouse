-- duplicates or null values ?

Select prd_id,
count(*) as prd_id_count
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;


-- we want to check if we have any products that are not included in the order ?


SELECT *,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7, len(prd_key)) as prd_key
from bronze.crm_prd_info
where substring(prd_key,7, len(prd_key)) not in(
SELECT sls_prd_key from bronze.crm_sales_details
);



-- Check for unwanted spaces

SELECT prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);


-- Check for nulls or negative numbers


select prd_cost from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;


-- Check for values where end date is less than the start dtae

select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;
