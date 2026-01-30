/*
================================================================================
CSV Bulk Insert Script
================================================================================
	
	Bulk Insert script that extracts data from specified CSV files
	Before extracting the data, the tables are truncated (emptied)

================================================================================
*/

-- bronze.crm_cust_info

-- TRUNCATE quickly deletes all rows from a table
TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_crm\cust_info.csv'
WITH (
	-- Skip first row (header)
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


-- bronze.crm_prd_info

TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_crm\prd_info.csv'
WITH (
	-- Skip first row (header)
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- bronze.crm_sales_details

TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details
FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_crm\sales_details.csv'
WITH (
	-- Skip first row (header)
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- bronze.erp_cust_az12

TRUNCATE TABLE bronze.erp_cust_az12;

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_erp\cust_az12.csv'
WITH (
	-- Skip first row (header)
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- bronze.erp_loc_a101

TRUNCATE TABLE bronze.erp_loc_a101;

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_erp\loc_a101.csv'
WITH (
	-- Skip first row (header)
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

-- bronze.erp_px_cat_g1v2

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Code\Data Engineering\Data Warehouse Project #1\datasets\source_erp\px_cat_g1v2.csv'
WITH (
	-- Skip first row (header)
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
