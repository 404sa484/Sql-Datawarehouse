-- Check if id from erp_px_cat_g1v2 matches with cat_id from crm_prd_info

select * from bronze.erp_px_cat_g1v2;
select * from silver.crm_prd_info

-- Check for unwanted spaces in cat and subcat

SELECT * FROM bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat)

-- Lets see the values of mainenance

select distinct maintenance from bronze.erp_px_cat_g1v2;
