/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

USE DataWarehouse;
GO

-- Create Stored Procedure
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @startTime DATETIME, @endTime DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY	
			SET @batch_start_time = GETDATE();
			PRINT '================================================';
			PRINT 'Loading Silver Layer';
			PRINT '================================================';

			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '------------------------------------------------';

		SET @startTime = GETDATE();
		PRINT '>> truncating table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> inserting data into : silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
				)
		-- Select the data from bronze schema for validating
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'  -- Using upper() for case sensitive
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- TRIM() for unwanted spaces
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'  
				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
				 ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM(
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL) AS t
			WHERE flag_last = 1;
			SET @endTime = GETDATE();
			PRINT '>> Time Taken to Load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
			PRINT '>>>>>>>>>>>>>';

		SET @startTime = GETDATE();
		PRINT '>> truncating table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> inserting data into : silver.crm_prd_info'
		-- insert the clensing data into silver_prd table[ last step]
		INSERT INTO silver.crm_prd_info (
		 prd_id,
		 cat_id,
		 prd_key,
		 prd_nm,
		 prd_cost,               
		 prd_line,               
		 prd_start_dt,              
		 prd_end_dt)   
		 SELECT
			   prd_id
			  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id  -- replace the '-' to '_' to join the table (ERP_Cat)
			  ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key  -- substring the table to join the table with CRM_sales_details
			  ,prd_nm
			  ,ISNULL(prd_cost, 0) AS prd_cost  --replace null values / USE COALESCE()
			  -- data transformation
			  ,CASE UPPER(TRIM(prd_line)) 
					WHEN 'M' THEN 'Mountain' -- details from source
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'  -- alternative CASE statement method when repetstive field
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'  -- handling NULL values
				END AS prd_line
				-- convert data type
			  ,CAST(prd_start_dt AS DATE) AS prd_start_dt
			   -- data enrichment
			  ,CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE
					) AS prd_end_dt_test -- for details check date validation code block
		FROM bronze.crm_prd_info;
		SET @endTime = GETDATE();
		PRINT '>> Time Taken to Load crm_prd_info: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>';


		SET @startTime = GETDATE()
		PRINT '>> truncating table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> inserting data into : silver.crm_sales_details'
		-- insert clensing data into silver.crm_sales data
		INSERT INTO silver.crm_sales_details(
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
		-- Load and clean crm_sales_details
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)  --Convert varchar before covert into date
				 END sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)  --Convert varchar before covert into date
				 END sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)  --Convert varchar before covert into date
				 END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price) 
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales/sls_quantity
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @endTime = GETDATE();
		PRINT '>> Time Taken to Load crm_sales_details: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>';

			PRINT '------------------------------------------------';
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------------------';

		SET @startTime = GETDATE();
		PRINT '>> truncating table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> inserting data into : silver.erp_cust_az12'
		--insert cleaned data into silver.erp_cust table
		INSERT INTO silver.erp_cust_az12(
					cid,
					bdate,
					gen
		)
		-- clean the ERP_cust data
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				 ELSE cid   -- Remove 'NAS' Prefix becouse for need to match other table string
			END AS cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS bdate,
			CASE WHEN TRIM(UPPER(gen)) IN ('F', 'Female') THEN 'Female'
				 WHEN TRIM(UPPER(gen)) IN ('M', 'Male') THEN 'Male'
				 ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12;
		SET @endTime = GETDATE();
		PRINT '>> Time Taken to Load erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>';

		SET @startTime = GETDATE()
		PRINT '>> truncating table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> inserting data into : silver.erp_loc_a101'
		-- insert cleaned data into ERP_loc table
		INSERT INTO silver.erp_loc_a101(
					cid,
					cntry
		)
		-- clean the ERP_loc data
		SELECT
			REPLACE(cid, '-', '') AS cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101;
		SET @endTime = GETDATE();
		PRINT '>> Time Taken to Load erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>';

		SET @startTime = GETDATE()
		PRINT '>> truncating table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> inserting data into : silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
					id,
					cat,
					subcat,
					maintenance
		)
		-- clean the ERP_loc data
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @endTime = GETDATE();
		PRINT '>> Time Taken to Load erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>';

		SET @batch_end_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver layer is completed';
		PRINT '>> Total Time Taken to Load Silver Layer: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================';
	 END TRY
	 BEGIN CATCH
			PRINT '================================================';
			PRINT 'Error Occurred While Loading Silver Layer';
			PRINT ERROR_MESSAGE();
			Print CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT '================================================';
		END CATCH
END;
GO
