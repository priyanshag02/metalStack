create or replace procedure gold.load_gold()
language plpgsql
as $$
begin

    -- creating customer dimension view and loading data into it

    begin
        create view gold.dim_customers as 
        select 
            row_number() over (order by ci.cst_id) as customer_key,
            ci.cst_id as customer_id,
            ci.cst_key as customer_number,
            ci.cst_firstname as firstname,
            ci.cst_lastname as lastname,
            ca.bdate as birth_date,
            cl.cntry as country,
            case 
                when ci.cst_gndr != 'n/a' then ci.cst_gndr
                else coalesce(ca.gen, 'n/a')
            end as gender, 
            ci.cst_marital_status as marital_status,
            ci.cst_create_date as date_created
        from silver.crm_cust_info ci
        left join silver.erp_cust_az12 ca 
        on ci.cst_key = ca.cid
        left join silver.erp_loc_a101 as cl 
        on ci.cst_key = cl.cid;  
    end;

    -- creating products dimension view and loading data into it

    begin
        create view gold.dim_products as
        select
            pi.prd_id as product_id,
            pi.prd_key as product_number,
            pi.prd_nm as product_name,
            pi.cat_id as category_id,
            pc.cat as category,
            pc.subcat as subcategory,
            pc.maintenance as maintenance,
            pi.prd_cost as cost,
            pi.prd_line as product_line,
            pi.prd_start_dt as start_date
        from silver.crm_prd_info as pi
        left join silver.erp_px_cat_g1v2 as pc
        on pi.cat_id = pc.id
        where pi.prd_end_dt is null;                 -- Filtering out historical data
    end;

    -- creating sales fact view and loading data into it

    begin
        create view gold.fact_sales as
        select
            sd.sls_ord_num as order_number,
            pdim.product_number as product_number,
            cdim.customer_key as customer_key,
            sd.sls_order_dt as order_date,
            sd.sls_ship_dt as shipping_date,
            sd.sls_due_dt as due_date,
            sd.sls_sales as sales_amount,
            sd.sls_quantity as quantity,
            sd.sls_price as price
        from silver.crm_sales_details sd
        left join gold.dim_products pdim
            on sd.sls_prd_key = pdim.product_number
        left join gold.dim_customers cdim
            on sd.sls_cust_id = cdim.customer_id;
    end;

end;
$$;
