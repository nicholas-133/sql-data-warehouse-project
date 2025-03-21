/*
====================================================
Data Exploring, Analyzing, and Cleaning BRONZE Tables
====================================================

Script Purpose:
	This script performs various audits for data consistency,
	accuracy, and standardization across the 'bronze' schemas.
	It includes audits for:
		- Null or duplicate primary keys.
		- Unwanted Spaces in string fiels.
		- Data standardization and consistency.
		- Invalid date ranges and orders.
		- Data consistency between related fields.
	
	The Script also provides a query solution for cleaning and prepping for
	migration to Silver Table (refer to Silver Stored Procedure where 
	this query is implemented).
*/
USE DataWarehouse;
GO

/*
----------------------------------------------------
Explore the data
----------------------------------------------------
*/
--sales details table
SELECT TOP (1000) *
  FROM [DataWarehouse].[bronze].[crm_sales_details] ;

--location table
SELECT TOP (1000) *
  FROM [DataWarehouse].[bronze].[erp_loc_a101] ;

--customer tables
SELECT TOP (1000) *
  FROM [DataWarehouse].[bronze].[crm_cust_info] ;

SELECT TOP (1000) *
  FROM [DataWarehouse].[bronze].[erp_cust_az12] ;


--product tables
SELECT TOP (1000) *
  FROM [DataWarehouse].[bronze].[crm_prd_info] ;

SELECT TOP (1000) *
  FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2] ;


/*
====================================================
Audit and Clean CRM_CUST_INFO Table
====================================================
*/

-- Check for Nulls or Duplicates in Primary Key
select
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL ;

-- Identify rows of the first duplicate
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466 ;

-- Retrieve the latest duplicate value for cst_id = 29466 
--    will repeat this logic for the other duplicate values
SELECT * 
FROM (
SELECT 
	*
	,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) t
WHERE flag_last = 1 AND cst_id = 29466 ;

--Check for unwanted spaces
	-- firstname
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) ;

	-- lastname
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname) ;

	-- gender
SELECT cst_gndr	
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr) ;

--Data Standardization and Consistency
	-- gender consistency 
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info ;

	-- marital consistency 
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info ;

/*
----------------------------------------------------
-- Cleaning Query
----------------------------------------------------
*/
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
-- Pull only the latest records to remove older duplicates 
FROM (
	SELECT 
		*
		,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
) t 
WHERE flag_last = 1  AND cst_id IS NOT NULL ;


/*
====================================================
Audit and Clean CRM_PRD_INFO Table
====================================================
*/

-- Check for nulls for duplicates in Primary Key
SELECT 
	prd_id
	,COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL ;

-- Check Category table for similarities
SELECT Distinct id from bronze.erp_px_cat_g1v2 ;

-- Check for unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info	
WHERE prd_nm != TRIM(prd_nm) ;

-- Check for Nulls or Negative Numbers
SELECT prd_cost
FROM bronze.crm_prd_info	
WHERE prd_cost < 0 OR prd_cost IS NULL ;

-- Data Standardization and Consistency
SELECT
	Distinct prd_line
FROM bronze.crm_prd_info ;

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info	
WHERE prd_end_dt < prd_start_dt ;;

-- Replace the end date with a more appropriate one that trails the next start date
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD( prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt ) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509') ;

/*
----------------------------------------------------
Cleaning Query
----------------------------------------------------
*/
SELECT 
	prd_id,
	REPLACE( SUBSTRING(prd_key, 1, 5), '-', '_' ) AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
 	prd_nm,
	ISNULL( prd_cost, 0 ) AS prd_cost,
	CASE
		WHEN UPPER( TRIM( prd_line ) ) = 'M' THEN 'Mountain'
		WHEN UPPER( TRIM( prd_line ) ) = 'R' THEN 'Road'
		WHEN UPPER( TRIM( prd_line ) ) = 'S' THEN 'Other Sales'
		WHEN UPPER( TRIM( prd_line ) ) = 'T' THEN 'Total Sales'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	CAST( prd_start_dt AS DATE) AS prd_start_dt,
	CAST( LEAD( prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt ) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
	SELECT sls_prd_key
	FROM bronze.crm_sales_details
) ;

/*
====================================================
Audit and Clean CRM_SALES_DETAILS Table
====================================================
*/

-- Check if there are any cust_id's that are not in the crm_cust_info table
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN ( SELECT cst_id FROM bronze.crm_cust_info) ;

-- Check for order date set to 0 or null 
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 ;

-- Check order date length consistency, while keeping previous logic
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) !=  8 ;

-- Check for Outliers (extremes in the order_dt), while keeping previous logic
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) !=  8 
OR sls_order_dt < 19000101 
OR sls_order_dt > 20500101 ;

-- Check for invalid order dates, while keeping previous logic
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt ;

-- Check Data Consistency: Between Sales, Quantity, and Price
	-- >> Sales = Quantity * Price
	-- >> Values must not be NULL, zero, or negative.
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price ;
-- Check with source system and/or expert if changes need to be made at the source

-- Rules for Proceeding with Cleaning:
	-- If Sales is negative, zero, or null, derive it using the Quantity and Price.
	-- If Price is zero or null, calculate it using Sales and Quantity.
	-- If Price is negative, convert it to a positive value.

/*
----------------------------------------------------
Cleaning Query
----------------------------------------------------
*/
SELECT 
	sls_ord_num,
	sls_prd_key,
    sls_cust_id,
    CASE 
		WHEN sls_order_dt = 0 OR LEN( sls_order_dt) != 8 THEN NULL
		ELSE CAST( CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LEN( sls_ship_dt ) != 8 THEN NULL
		ELSE CAST( CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN( sls_due_dt ) != 8 THEN NULL
		ELSE CAST( CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END AS sls_due_dt,
    CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price) -- calculating correct sales when price is negative
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE
		WHEN sls_price is NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details ;


/*
====================================================
Audit and Clean ERP_CUST_AZ Table
====================================================
*/

-- Audit
SELECT 
	cid,
	bdate,
	gen
FROM bronze.erp_cust_az12 ;

-- Check similarities with crm_cust_info table 
SELECT * FROM [bronze].[crm_cust_info] ;

-- Check outlier dates (extremes)
SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE() ;
--- There are some bad dates, consider checking with source system

-- Data Standardization and Consistency
SELECT DISTINCT 
	gen	
FROM bronze.erp_cust_az12 ;

-- Fix Gender Column
SELECT DISTINCT 
	gen,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('m', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12 ;

/*
----------------------------------------------------
Cleaning Query ERP_CUST_AZ
----------------------------------------------------
*/

SELECT
	CASE 
		WHEN cid LIKE 'NAS&' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid, -- substring matched data from the crm_cust_info Table
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('m', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12 ;

/*
====================================================
Audit and Clean ERP_LOC_A101 Table
====================================================
*/

-- Audit
SELECT 
	cid,
	cntry
FROM bronze.erp_loc_a101
-- Compare with crm_cust_info table
SELECT cst_key FROM bronze.crm_cust_info ;

-- Data Standardization and Consistency
SELECT DISTINCT
	cntry AS old_cntry,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) = 'US' THEN 'United States'
		WHEN TRIM(cntry) = 'USA' THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry -- replace acronyms with correct country name
FROM bronze.erp_loc_a101
ORDER BY cntry ;


/*
----------------------------------------------------
Cleaning Query ERP_LOC_A101
----------------------------------------------------
*/

SELECT
	REPLACE(cid, '-', '') cid, -- replace cid to match crm_cust_info table
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) = 'US' THEN 'United States'
		WHEN TRIM(cntry) = 'USA' THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry -- replace acronyms with correct country name
FROM bronze.erp_loc_a101 ;


/*
====================================================
Audit and Clean ERP_PX_CAT_G1V2 Table
====================================================
*/

-- Audit
SELECT
	*
FROM bronze.erp_px_cat_g1v2

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization and Consistency
SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2


/*
----------------------------------------------------
Cleaning Query
----------------------------------------------------
*/

-- Bronze Query already in a cleaned state proceed to insert as is for consistency in workflow
SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

