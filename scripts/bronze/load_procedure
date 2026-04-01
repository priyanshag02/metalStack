create or replace procedure bronze.load_bronze()
language plpgsql
as $$
declare
load_time timestamp;  -- declare a timestamp variable
begin
-- capture the start time
load_time := now();

-- truncating all the tables before loading
truncate table bronze.crm_cust_info;
truncate table bronze.crm_sales_details;
truncate table bronze.crm_prd_info;
truncate table bronze.erp_cust_az12;
truncate table bronze.erp_loc_a101;
truncate table bronze.erp_px_cat_g1v2;

-- load crm_cust_info
begin
copy bronze.crm_cust_info
from 'c:/users/priyanshagarwal/downloads/dwh_project/datasets/source_crm/cust_info.csv'
delimiter ','
csv header;
raise notice '[%] crm_cust_info loaded successfully', load_time;
exception
when others then
raise notice '[%] error loading crm_cust_info: %', load_time, sqlerrm;
end;

-- load crm_sales_details
begin
copy bronze.crm_sales_details
from 'c:/users/priyanshagarwal/downloads/dwh_project/datasets/source_crm/sales_details.csv'
delimiter ','
csv header;
raise notice '[%] crm_sales_details loaded successfully', load_time;
exception
when others then
raise notice '[%] error loading crm_sales_details: %', load_time, sqlerrm;
end;

-- load crm_prd_info
begin
copy bronze.crm_prd_info
from 'c:/users/priyanshagarwal/downloads/dwh_project/datasets/source_crm/prd_info.csv'
delimiter ','
csv header;
raise notice '[%] crm_prd_info loaded successfully', load_time;
exception
when others then
raise notice '[%] error loading crm_prd_info: %', load_time, sqlerrm;
end;

-- load erp_cust_az12
begin
copy bronze.erp_cust_az12
from 'c:/users/priyanshagarwal/downloads/dwh_project/datasets/source_erp/cust_az12.csv'
delimiter ','
csv header;
raise notice '[%] erp_cust_az12 loaded successfully', load_time;
exception
when others then
raise notice '[%] error loading erp_cust_az12: %', load_time, sqlerrm;
end;

-- load erp_loc_a101
begin
copy bronze.erp_loc_a101
from 'c:/users/priyanshagarwal/downloads/dwh_project/datasets/source_erp/loc_a101.csv'
delimiter ','
csv header;
raise notice '[%] erp_loc_a101 loaded successfully', load_time;
exception
when others then
raise notice '[%] error loading erp_loc_a101: %', load_time, sqlerrm;
end;

-- load erp_px_cat_g1v2
begin
copy bronze.erp_px_cat_g1v2
from 'c:/users/priyanshagarwal/downloads/dwh_project/datasets/source_erp/px_cat_g1v2.csv'
delimiter ','
csv header;
raise notice '[%] erp_px_cat_g1v2 loaded successfully', load_time;
exception
when others then
raise notice '[%] error loading erp_px_cat_g1v2: %', load_time, sqlerrm;
end;

end;
$$;
