/*
===================================================================================
Quality Checks
===================================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization across the 'silver' schemas. it includes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fields.

Usage Notes:
	- Run these checks after data loading silver layer.
	- Investigate and resolve any discrepancies found during the checks.
===================================================================================
*/

-- =====================================================================
-- Checking 'silver.crm_cust_info'
-- =====================================================================
-- Check for NULLs or Duplicates in Primary key
-- Expectation: No Results


-- =====================================================================
-- Checking 'silver.crm_cust_info'
-- =====================================================================
		-- Check for Nulls or Duplicates in Primary Key
		-- Expectation: No Result

		SELECT
		cst_id,
		COUNT(*)
		FROM bronze.crm_cust_info
		GROUP BY cst_id
		HAVING COUNT(*) > 1 or cst_id IS NULL


		-- Check for unwanted Spaces
		-- Expectation: No Result
		SELECT cst_key
		FROM bronze.crm_cust_info
		WHERE cst_key != TRIM(cst_key)

		-- Full code
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status,

		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM (
			SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last =1


		-- Data Standardization & Consistency
		SELECT DISTINCT cst_gndr
		FROM bronze.crm_cust_info


-- =====================================================================
-- Checking 'silver.crm_prd_info'
-- =====================================================================
		-- Check for Nulls or Duplicates in Primary Key
		-- Expectation: No Result
		SELECT
		prd_id,
		COUNT(*)
		FROM bronze.crm_prd_info
		GROUP BY prd_id
		HAVING COUNT(*) > 1 or prd_id IS NULL

		-- Check for unwanted spaces
		-- Expectation: No Results
		SELECT prd_nm
		FROM bronze.crm_prd_info
		WHERE prd_nm != TRIM(prd_nm)

		-- Check for Nulls or Negative Numbers
		-- Expectation: No Results
		SELECT prd_cost
		FROM bronze.crm_prd_info
		WHERE prd_cost < 0 OR prd_cost IS NULL

		-- Data Standardization & Consistency
		SELECT DISTINCT prd_line
		FROM bronze.crm_prd_info

		-- Check for Invalid Date Orders
		SELECT *
		FROM bronze.crm_prd_info
		WHERE prd_end_dt < prd_start_dt

		SELECT * FROM bronze.crm_prd_info


-- =====================================================================
-- Checking 'silver.crm_sales_details'
-- =====================================================================
		SELECT 
		[sls_ord_num],
		[sls_prd_key],
		[sls_cust_id],
		CASE WHEN [sls_order_dt] = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN [sls_ship_dt] = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN [sls_due_date] = 0 OR LEN(sls_due_date) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_date AS VARCHAR) AS DATE)
		END AS sls_due_date,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		[sls_quantity],
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
		  FROM [bronze].[crm_sales_details]


		  -- Check the columns if they match with the other primary keys
  
		  -- Check for Invalid Dates
		  SELECT
		  sls_order_dt
		  FROM bronze.crm_sales_details
		  WHERE sls_order_dt <= 0
		  -- Where there is 0 change to NULL
		  SELECT
		  NULLIF(sls_order_dt,0) sls_order_dt
		  FROM bronze.crm_sales_details
		  WHERE sls_order_dt <= 0 
		  OR LEN(sls_order_dt) != 8
		  OR sls_order_dt > 20500101
		  OR sls_order_dt < 19000101

		  -- Check for Invalid Date Orders

		  SELECT
		  *
		  FROM bronze.crm_sales_details
		  WHERE sls_order_dt > sls_ship_dt  OR sls_order_dt > sls_due_date

		  -- Check Date consistency: Between Sales, Quantity, and Price
		  -- >> Sales = Quantity * Price
		  -- >> Values must not be NULL, Zero, or Negative.

		  SELECT DISTINCT
		  sls_sales AS old_sls_sales,
		  sls_quantity,
		  sls_price AS old_sls_price,
		  CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details
		WHERE sls_sales != sls_quantity * sls_price
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
		ORDER BY   sls_sales, sls_quantity, sls_price



-- =====================================================================
-- Checking 'silver.erp_cust_az12'
-- =====================================================================
		SELECT
		cid,
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12

		-- Identify Out-of-Range Dates

		SELECT DISTINCT
		bdate
		FROM bronze.erp_cust_az12
		WHERE bdate < '1924-01-01' OR bdate > GETDATE()

		-- Data Standardization & Consistency

		SELECT DISTINCT 
		gen,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12



-- =====================================================================
-- Checking 'silver.erp_loc_a101'
-- =====================================================================
		SELECT
		REPLACE (cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101

		-- Data Standardization & Consistency
		SELECT DISTINCT cntry
		FROM bronze.erp_loc_a101
		ORDER BY cntry



-- =====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- =====================================================================
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2

		-- Check for unwanted spaces
		SELECT * FROM bronze.erp_px_cat_g1v2
		WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

		-- Check Data Standardization & Consistency
		SELECT DISTINCT
		maintenance
		FROM bronze.erp_px_cat_g1v2
