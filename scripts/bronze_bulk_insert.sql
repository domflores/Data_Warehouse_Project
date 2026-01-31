/*
================================================================================
CSV Bulk Insert Script
================================================================================
	
	Bulk Insert script that extracts data from specified CSV files
	Before extracting the data, the tables are truncated (emptied)

================================================================================
*/

-- TRUNCATE quickly deletes all rows from a table

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	-- VARIABLE DECLARATION
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '========================================';
		PRINT 'LOADING Bronze Layer'
		PRINT '========================================';

		PRINT '----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------';

		-- Get start time
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_crm\cust_info.csv'
		WITH (
			-- Skip first row (header)
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Get end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		-- Get start time
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_crm\prd_info.csv'
		WITH (
			-- Skip first row (header)
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Get end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		-- Get start time
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_crm\sales_details.csv'
		WITH (
			-- Skip first row (header)
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Get end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '----------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------';

		-- Get start time
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_erp\cust_az12.csv'
		WITH (
			-- Skip first row (header)
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Get end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		-- Get start time
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_erp\loc_a101.csv'
		WITH (
			-- Skip first row (header)
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Get end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		-- Get start time
		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			-- Skip first row (header)
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Get end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		-- Get total end time
		SET @batch_end_time = GETDATE();

		-- Print Total Time
		PRINT '========================================';
		PRINT '>> Total Time to Load Bronze Layer: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR);
		PRINT '========================================';

	END TRY
	BEGIN CATCH
		PRINT '========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '========================================';
	END CATCH
END
