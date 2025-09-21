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


SELECT * 
FROM bronze.crm_prd_info;

EXEC xp_fileexist '/tmp/source_crm/cust_info.csv';

EXEC xp_dirtree '/tmp/source_crm', 1, 1;

*/
/*
----------------------------------------
------------ Quality Checks ------------
----------------------------------------

--------------------
-- Check Duplicates
--------------------

SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1; 

-- Removing Duplicates

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;   -- From 3 duplicates Keep the latest info

SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

SELECT *
FROM 
(
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t WHERE flag_last != 1;  -- Gives you all the info u need to remove

SELECT *
FROM
(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t WHERE flag_last = 1;   -- Gives u clean records without duplicates


------------------------
-- Check Unwanted Spaces
------------------------

-- Expectation: No Results
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT 
cst_id,
cst_key,
TRIM(cst_lastname) AS cst_lastname,
TRIM(cst_gndr) AS cst_gndr,
cst_marital_status,
cst_create_date
FROM
(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t WHERE flag_last = 1;   -- Gives u clean records without duplicates

-------------------------
-- Check Data Consistency
-------------------------

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


SELECT 
cst_id,
cst_key,
TRIM(cst_lastname) AS cst_lastname,
TRIM(cst_gndr) AS cst_gndr,
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
FROM
(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t WHERE flag_last = 1;   -- Gives u clean records without duplicates


-- Inserting into Table

INSERT INTO silver.crm_cust_info
(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
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
FROM
(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t WHERE flag_last = 1;   -- Gives u clean records without duplicates

--------------------------
-- Checks For Silver Table
--------------------------

--------------------
-- Check Duplicates
--------------------

SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 


------------------------
-- Check Unwanted Spaces
------------------------

-- Expectation: No Results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-------------------------
-- Check Data Consistency
-------------------------

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;


----------------------------------------
------------ Quality Checks ------------
----------------------------------------

-------------------------
-- crm_prd_info
-------------------------


--------------------
-- Check Duplicates
--------------------

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; 


------------------------
-- prd_id = cat_id + prd_key
------------------------
SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
    (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN 
    (SELECT sls_prd_key FROM bronze.crm_sales_details);   -- just products with no orders


------------------------
-- Check Unwanted Spaces
------------------------

-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


---------------------------------
-- Check NULL or Negative Numbers
---------------------------------

-- Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-------------------------
-- Check Data Consistency
-------------------------

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;   -- Give full names to these abbreviations

SELECT 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE 
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN 
    (SELECT sls_prd_key FROM bronze.crm_sales_details);   -- just products with no orders

--------------------------------
-- Check for Invalid Date Orders
--------------------------------
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;   -- End Date must not be Earlier than the Start Date

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


INSERT INTO silver.crm_prd_info
(
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE 
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN 
    (SELECT sls_prd_key FROM bronze.crm_sales_details);   -- just products with no orders

--------------------------
-- Checks For Silver Table
--------------------------

--------------------
-- Check Duplicates
--------------------

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; 


------------------------
-- Check Unwanted Spaces
------------------------

-- Expectation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-------------------------
-- Check Data Consistency
-------------------------

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

--------------------------------
-- Check for Invalid Date Orders
--------------------------------
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt; 

SELECT *
FROM silver.crm_prd_info;

----------------------------------------
------------ Quality Checks ------------
----------------------------------------

---------------------
-- crm_sales_details
---------------------

SELECT 
sls_ord_nm,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_nm != TRIM(sls_ord_nm);

SELECT 
sls_ord_nm,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT 
sls_ord_nm,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

--------------------------
-- Check for Invalid Dates
--------------------------

-- Check for Zeros and Negative values

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0;     -- Zeros Found

-- Replace Zeros with NULL

SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0; 

-- Check for LEngth of Date

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101; 

SELECT 
sls_ord_nm,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE)
END AS sls_order_dt,
CASE 
    WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE)
END AS sls_ship_dt,
CASE 
    WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

-- Check for Order_dt < Shipping_dt / Due_dt

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;  -- None Found

--------------------------------------------------------
-- Check Data Consistency: BTW Sales, Quantity and Price
--------------------------------------------------------

-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero or negative

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Fixing it

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
CASE 
    WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


INSERT INTO silver.crm_sales_details
(
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
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE)
END AS sls_order_dt,
CASE 
    WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE)
END AS sls_ship_dt,
CASE 
    WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE)
END AS sls_due_dt,
CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE 
    WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;

--------------------------
-- Checks For Silver Table
--------------------------

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt < sls_due_dt; 

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


----------------------------------------
------------ Quality Checks ------------
----------------------------------------

-----------------
-- erp_cust_az12
-----------------

SELECT 
cid,
bdate,
gen 
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%';  -- 1 invalid value

SELECT * FROM [silver].[crm_cust_info];

SELECT 
cid,
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
bdate,
gen 
FROM bronze.erp_cust_az12
WHERE CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-------------------------------
-- Check for Out of Range Dates
-------------------------------

SELECT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' AND bdate > GETDATE();

SELECT 
cid,
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
CASE 
    WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
gen 
FROM bronze.erp_cust_az12
WHERE CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-------------------------------------
-- Data Standardization & Consistency
-------------------------------------

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

SELECT DISTINCT gen,
CASE 
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE', 'Female') THEN 'Female'
    WHEN UPPER(TRIM(gen)) In ('M', 'MALE', 'Male') THEN 'Male'
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- Check ASCII values of each character
SELECT 
    gen,
    ASCII(SUBSTRING(gen, 1, 1)) AS char1_ascii,
    ASCII(SUBSTRING(gen, 2, 1)) AS char2_ascii,
    ASCII(SUBSTRING(gen, 3, 1)) AS char3_ascii,
    LEN(gen) AS length,
    DATALENGTH(gen) AS data_length
FROM bronze.erp_cust_az12
GROUP BY gen;

SELECT DISTINCT gen,
CASE 
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(32), ''), CHAR(9), '')) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(32), ''), CHAR(9), '')) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END AS cleaned_gen
FROM bronze.erp_cust_az12;


INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT 
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END cid,
CASE 
    WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
CASE 
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(32), ''), CHAR(9), '')) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(32), ''), CHAR(9), '')) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

--------------------------
-- Checks For Silver Table
--------------------------

SELECT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' AND bdate > GETDATE();

SELECT DISTINCT
gen
FROM silver.erp_cust_az12;

SELECT *
FROM silver.erp_cust_az12;

----------------------------------------
------------ Quality Checks ------------
----------------------------------------

---------------
-- erp_loc_a101
---------------

SELECT cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;  -- Key has no separators

SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101;

-- Check Integrity in both tables

SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN 
(SELECT cst_key FROM silver.crm_cust_info);   -- Should return enpty


SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


SELECT 
    cntry,
    LEN(cntry) AS length,
    DATALENGTH(cntry) AS data_length,
    ASCII(SUBSTRING(cntry, 1, 1)) AS char1_ascii,
    ASCII(SUBSTRING(cntry, 2, 1)) AS char2_ascii,
    ASCII(SUBSTRING(cntry, 3, 1)) AS char3_ascii,
    ASCII(SUBSTRING(cntry, 4, 1)) AS char4_ascii,
    ASCII(SUBSTRING(cntry, 5, 1)) AS char5_ascii,
    ASCII(SUBSTRING(cntry, 6, 1)) AS char6_ascii,
    ASCII(SUBSTRING(cntry, 7, 1)) AS char7_ascii,
    ASCII(SUBSTRING(cntry, 8, 1)) AS char8_ascii,
    ASCII(SUBSTRING(cntry, 9, 1)) AS char9_ascii,
    ASCII(SUBSTRING(cntry, 10, 1)) AS char10_ascii
FROM bronze.erp_loc_a101
GROUP BY cntry
ORDER BY cntry;

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT DISTINCT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(32), '')) = 'DE' THEN 'Germany'
        WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(32), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL OR 
             REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(32), '') = '' THEN 'n/a'
        ELSE REPLACE(REPLACE(TRIM(cntry), CHAR(13), ''), CHAR(32), '')
    END AS cleaned_cntry
FROM bronze.erp_loc_a101;

--------------------------
-- Checks For Silver Table
--------------------------

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

----------------------------------------
------------ Quality Checks ------------
----------------------------------------

------------------
-- erp_px_cat_g1v2
------------------

SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

----------------------------
-- Check for Unwanted Spaces
----------------------------
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);    -- no spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);    -- no spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance);    -- no spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance); 

-------------------------------------
-- Data Standardization & Consistency
-------------------------------------

SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2;  -- Nothing to change

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2;  -- Nothing to change

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2; 

-- Check for hidden characters
SELECT 
    maintenance,
    LEN(maintenance) AS length,
    DATALENGTH(maintenance) AS data_length,
    ASCII(SUBSTRING(maintenance, 1, 1)) AS char1_ascii,
    ASCII(SUBSTRING(maintenance, 2, 1)) AS char2_ascii,
    ASCII(SUBSTRING(maintenance, 3, 1)) AS char3_ascii,
    ASCII(SUBSTRING(maintenance, 4, 1)) AS char4_ascii
FROM bronze.erp_px_cat_g1v2
GROUP BY maintenance;

INSERT INTO silver.erp_px_cat_g1v2 (cat, subcat, maintenance)
SELECT 
cat,
subcat,
CASE 
    WHEN UPPER(REPLACE(REPLACE(TRIM(maintenance), CHAR(13), ''), CHAR(10), '')) IN ('YES', 'Y') THEN 'Yes'
    WHEN UPPER(REPLACE(REPLACE(TRIM(maintenance), CHAR(13), ''), CHAR(10), '')) IN ('NO', 'N') THEN 'No'
    WHEN TRIM(maintenance) = '' OR maintenance IS NULL THEN 'n/a'
    ELSE TRIM(maintenance)
END AS maintenance
FROM bronze.erp_px_cat_g1v2; 

--------------------------
-- Checks For Silver Table
--------------------------

SELECT DISTINCT
maintenance
FROM silver.erp_px_cat_g1v2; 
*/
-------------------------------------------------------------------------
------------------------ Silver Stored Procedure ------------------------
-------------------------------------------------------------------------

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

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
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
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
