create or replace procedure silver.load_procedure()
language plpgsql
as $$

begin 

-- truncating all the tables before loading
truncate table silver.crm_cust_info;
truncate table silver.crm_sales_details;
truncate table silver.crm_prd_info;
truncate table silver.erp_cust_az12;
truncate table silver.erp_loc_a101;
truncate table silver.erp_px_cat_g1v2;

--loading silver.crm_cust_info
begin
insert into silver.crm_cust_info (
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
    replace(cst_key, 'AW', 'AW_'),
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        when upper(trim(cst_marital_status)) = 'S' then 'Single'
        else 'Separated'
    end as cst_marital_status,                          -- Normalizing marital status values
    case 
        when upper(trim(cst_gndr)) = 'M' then 'Male'
        when upper(trim(cst_gndr)) = 'F' then 'Female'
        else 'Others'
    end as cst_gndr,                                    -- Normalizing gender values
    cst_create_date
from (
    select *,
            row_number() over (partition by cst_id order by cst_create_date desc) as entry_flag
            from bronze.crm_cust_info
            where cst_id is not null
) t
where entry_flag = 1;                                   -- Selecting most recent record per customer
end;

-- loading silver.crm_prd_info
begin
insert into silver.crm_prd_info (
    prd_id,
    prd_key,
    prd_nm,
    cat_id,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select
    prd_id,
    substring(prd_key from 7) as prd_key,                       -- Extracting product key
    prd_nm,
    replace(substring(prd_key, 1, 5), '-', '_') as cat_id,      -- Extracting category ID
    case 
        when prd_cost < 0 then -1*prd_cost
        when prd_cost is null then 0
        else prd_cost
    end as prd_cost,                                            -- Transformation into proper pricing
    case upper(trim(prd_line))
        when 'M' then 'Mountain'
        when 'R' then 'Road'
        when 'T' then 'Touring'
        when 'S' then 'Other Sales'
    end as prd_line,                                            -- Mapping product line to descriptive values
    cast(prd_start_dt as date) as prd_start_dt,                 -- Transformation of start date
    cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) 
        as prd_end_dt                                           -- Normalizing end date
from bronze.crm_prd_info;
end;

-- loading silver.crm_sales_details
begin
insert into silver.crm_sales_details (
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
        when sls_order_dt = 0 or length(sls_order_dt::text) != 8 then null
        else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,                                            -- Transformation into valid order date
    case 
        when sls_ship_dt = 0 or length(sls_ship_dt::text) != 8 then null
        else cast(cast(sls_ship_dt as varchar) as date) 
    end as sls_ship_dt,                                             -- Transformation into valid shipping date
    case 
        when sls_due_dt = 0 or length(sls_due_dt::text) != 8 then null
        else cast(cast(sls_due_dt as varchar) as date)
    end as sls_due_dt,                                              -- Transformation into valid due date
    case 
        when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity*abs(sls_price)
            then sls_quantity*abs(sls_price)
        else sls_sales
    end as sls_sales,                                               -- Calculation of correct sales value
    sls_quantity,
    case
        when sls_price <= 0 or sls_price is null then sls_sales/nullif(sls_quantity, 0)
        else sls_price
    end as sls_price                                                -- Calculation of correct price value
from bronze.crm_sales_details;
end;

-- loading erp_cust_az12
begin
insert into silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
select 
    replace(case 
        when cid like 'NAS%' then substring(cid from 4)
        else cid
    end, 'AW', 'AW_'
    ) as cid,
    case 
        when bdate > now() then null
        else bdate
    end as bdate,
    case 
        when upper(trim(gen)) in ('F', 'Female') then 'Female'
        when upper(trim(gen)) in ('M', 'Male') then 'Male'
        else 'Others'
    end as gen
from bronze.erp_cust_az12;
end;

-- loading erp_loc_a101
begin
insert into silver.erp_loc_a101 (
    cid,
    cntry
)
select 
    replace(cid, '-', '_') as cid,
    case 
        when upper(trim(cntry)) = 'DE' then 'Germany'
        when upper(trim(cntry)) in ('US', 'USA') then 'United States'
        when trim(cntry) = '' or cntry is null then 'Others'
        else initcap(trim(cntry))
    end as cntry
from bronze.erp_loc_a101;
end;

-- loading silver.erp_px_cat_g1v2
begin
insert into silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
select
    id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;
end;

end;
$$;
