/*
====================================================
Quality Checks for Silver Schema
====================================================

Script Purpose:
	This script performs variuos quality checks for data consistency,
	accuracy, and standardization across the 'silver' schemas. It includes
	checks for:
		- Null or duplicate primary keys.
		- Unwanted Spaces in string fiels.
		- Data standardization and consistency.
		- Invalid date ranges and orders.
		- Data consistency between related fields.
	Usage:
		- Run these checks after data loading Silver Layer.
		- Investigate and resolve any discrepancies found during the checks.
*/

/*
====================================================
Audit CRM_CUST_INFO Table
====================================================
*/

-- Check for Nulls or Duplicates in Primary Key
select
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL ;

--Check for unwanted spaces
	-- firstname
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) ;

	-- lastname
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname) ;

	-- gender
SELECT cst_gndr	
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr) ;


--Data Standardization and Consistency
	-- gender consistency 
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info ;

	-- marital consistency 
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info ;


/*
====================================================
Audit CRM_PRD_INFO Table
====================================================
*/

-- Check for nulls for duplicates in Primary Key
SELECT 
	prd_id
	,COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL ;

-- Check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info	
WHERE prd_nm != TRIM(prd_nm) ;

-- Check for Nulls or Negative Numbers
SELECT prd_cost
FROM silver.crm_prd_info	
WHERE prd_cost < 0 OR prd_cost IS NULL ;

-- Data Standardization and Consistency
SELECT
	Distinct prd_line
FROM silver.crm_prd_info ;

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info	
WHERE prd_end_dt < prd_start_dt ;


/*
====================================================
Audit Silver Table: CRM_SALES_DETAILS
====================================================
*/

-- Check if there are any cust_id's that are not in the crm_cust_info table
SELECT * 
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN ( SELECT cst_id FROM silver.crm_cust_info) ;

-- Check for Outliers (extremes in the order_dt)
SELECT
	sls_order_dt
FROM silver.crm_sales_details
WHERE 
sls_order_dt < '1900-01-01'
OR sls_order_dt > '2050-01-01' ;

-- Check for invalid order dates
SELECT
	sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt ;

-- Check Data Consistency: Between Sales, Quantity, and Price
	-- >> Sales = Quantity * Price
	-- >> Values must not be NULL, zero, or negative.
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price ;


/*
====================================================
Audit Silver Table: ERP_CUST_AZ12
====================================================
*/

-- Check for Null Id values
SELECT
	*
FROM silver.erp_cust_az12
WHERE cid IS NULL ;

-- Check outlier dates (extremes)
SELECT DISTINCT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1914-01-01' OR bdate > GETDATE() ;

-- Data Standardization and Consistency
SELECT DISTINCT 
	gen	
FROM silver.erp_cust_az12 ;



/*
====================================================
Audit Silver Table: ERP_LOC_A101
====================================================
*/

-- Check for Null Values
SELECT
	*
FROM silver.erp_loc_a101
WHERE cid IS NULL ;

-- Data Standardization and Consistency
SELECT DISTINCT
	cntry AS old_cntry
FROM silver.erp_loc_a101 ;



/*
====================================================
Audit Silver Table: ERP_PX_CAT_G1V2
====================================================
*/

-- Check for Null Values
SELECT
	*
FROM silver.erp_px_cat_g1v2
WHERE id IS NULL
OR cat IS NULL ;

-- Check for unwanted spaces
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) ;

-- Data Standardization and Consistency
SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2 ;