---Queries to Create table 

create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
)





create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);




create table bronze.crm_sales_details(
sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

create table bronze.erp_cust_Az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);


create table bronze.erp_Loc_A101(
cid    NVARCHAR(50),
cntry  NVARCHAR(50)
);



create table bronze.erp_px_catat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);









----Queries to insert data


bulk insert bronze.crm_cust_info
from 'C:\Users\gandh\OneDrive\Documents\Downloads\OneDrive\Documents\SQL datawarehouse project\datasets\source_crm\cust_info.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
)


bulk insert bronze.crm_prd_info
from 'C:\Users\gandh\OneDrive\Documents\Downloads\OneDrive\Documents\SQL datawarehouse project\datasets\source_crm\prd_info.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
)

bulk insert bronze.crm_sales_details
from 'C:\Users\gandh\OneDrive\Documents\Downloads\OneDrive\Documents\SQL datawarehouse project\datasets\source_crm\sales_details.csv'
with(
firstrow=2,
fieldterminator=',',
tablock


  bulk insert bronze.erp_cust_Az12
from 'C:\Users\gandh\OneDrive\Documents\Downloads\OneDrive\Documents\SQL datawarehouse project\datasets\source_erp\cust_Az12.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
)


  bulk insert bronze.erp_loc_a101
from 'C:\Users\gandh\OneDrive\Documents\Downloads\OneDrive\Documents\SQL datawarehouse project\datasets\source_erp\loc_a101.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
)


  bulk insert bronze.erp_px_catat_g1v2
from 'C:\Users\gandh\OneDrive\Documents\Downloads\OneDrive\Documents\SQL datawarehouse project\datasets\source_erp\px_cat_g1v2.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
)




