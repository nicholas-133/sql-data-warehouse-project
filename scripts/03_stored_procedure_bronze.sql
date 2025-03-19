
/* 
========================================
Create Stored Procedures
========================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	DECLARE 
		@batch_start_time DATETIME,
		@batch_end_time DATETIME,
		@start_time DATETIME,
		@end_time DATETIME
		;
	
	BEGIN TRY
		-- Setting batch_start_time
		SET @batch_start_time = GETDATE();
		
		PRINT '==================================='
		PRINT 'Loading Bronze Layer'
		PRINT '==================================='
		PRINT ' '
		/*
		===================
		Insert CSVs into tables
		===================
		*/
		PRINT '-----------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '-----------------------------------'
		-- customer info table


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Insterting Table: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		-- pull csv from export folder
		FROM 'C:\Users\Nicholas\Documents\Github_Export\datasets_export_direct\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time ) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------'
		PRINT ' '

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		-- prd info table
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Insterting Table: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Nicholas\Documents\Github_Export\datasets_export_direct\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time ) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------'
		PRINT ' '

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		-- sales details table
		TRUNCATE TABLE bronze.crm_sales_details;
	
		PRINT '>> Insterting Table: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Nicholas\Documents\Github_Export\datasets_export_direct\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time ) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------'
		PRINT ' '

		PRINT '-----------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '-----------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		-- CUST_AZ12 table
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Insterting Table: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Nicholas\Documents\Github_Export\datasets_export_direct\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time ) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------'
		PRINT ' '

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		-- LOC_A101 table
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Insterting Table: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Nicholas\Documents\Github_Export\datasets_export_direct\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time ) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------'
		PRINT ' '

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		-- PX_CAT_G1V2 table
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		PRINT '>> Insterting Table: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Nicholas\Documents\Github_Export\datasets_export_direct\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2 ,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time ) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------'
		PRINT ' '

		-- Setting batch_end_time
		SET @batch_end_time = GETDATE();
		
		PRINT '=========================================================='
		PRINT 'Loading Bronze Layer is Complete';
		PRINT '    - Total Load Duration: ' + CAST( DATEDIFF( second, @batch_start_time, @batch_end_time) AS NVARCHAR ) + ' seconds';
		PRINT '=========================================================='

	END TRY
	BEGIN CATCH
		PRINT '=========================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST( ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================================='
	END CATCH

END
