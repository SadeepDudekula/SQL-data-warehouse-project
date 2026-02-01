/*
==============================================================================
DDL Script: Create Gold Views
===============================================================================

Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
================================================================================
*/

-- ============================================================
-- Create dimention : gold.dim_customers
-- ============================================================
if object.id('gold.dim_customer','v') is not null
	drop view gold.dim_customer

go

create view gold.dim_customer as
select
	row_number() over(order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	cl.cntry as country,
	ci.cst_material_status as marital_status,
	case when ci.cst_gndr  != 'n/a' then ci.cst_gndr
	else coalesce(cb.gen,'n/a')
	end as gender,
	cb.bdate as birth_date,
	ci.cst_create_date as create_date	
from silver.crm_cust_info ci
left join silver.erp_cust_az12 cb
on ci.cst_key = cb.cid
left join silver.erp_loc_a101 cl
on ci.cst_key = cl.cid

go

-- ============================================================
-- Create dimention : gold.dim_product
-- ============================================================
if object.id('gold.dim_product','v') is not null
	drop view gold.dim_product

go

create view gold.dim_product as

select 
	row_number() over (order by prd_id) as product_key,
	prd_id as product_id,
	prd_key as product_number,
	prd_nm as product_name,
	cat_id as category_id,
	cat as category,
	subcat as sub_category,
	prd_cost as cost,
	prd_line as product_line,
	maintenance ,
	prd_start_dt as start_date
	from(

select
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	row_number() over (partition by prd_key order by prd_key) flg,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null
)t where flg =1

go

-- ============================================================
-- Create dimention : gold.fact_sales
-- ============================================================
if object.id('gold.fact_sales','v') is not null
	drop view gold.fact_sales
go

create view gold.fact_sales as
select
sd.sls_ord_num as order_number,
pr.product_key ,
cu.customer_key,
sd.sls_due_dt as due_date,
sd.sls_order_dt as order_date,
sd.sls_ship_dt ship_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_product pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id = cu.customer_id

go
