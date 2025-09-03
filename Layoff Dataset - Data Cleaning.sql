-- -------------------------------------------------------------------------------------------------------------------
--                                                   DATA CLEANING
-- -------------------------------------------------------------------------------------------------------------------

-- select the database
USE world_layoffs;

-- check the tables present
SHOW TABLES;

-- look into the table contents
SELECT * FROM layoffs;

-- creating a copy of the original raw dataset
CREATE TABLE layoffs_copy
LIKE layoffs;

-- inserting into the new table the entries from original table
INSERT INTO layoffs_copy
SELECT * FROM layoffs;

-- checking the copy table
SELECT * FROM layoffs_copy;
-- now we have the dataset copied and we can now process it




-- ------------------------------------------------------------------------------------------------------------------------
-- ------------------------------------- STEPS FOLLOWED FOR THE DATA CLEANING PROCESS -------------------------------------
-- When cleaning the dataset, we will follow these steps:
-- 1. Check for duplicates using ROW_NUMBER() and remove them
-- 2. Standardize the data (fix inconsistent country names, trim spaces, clean symbols, format dates)
-- 3. Handle missing values (replace blanks with NULL, impute where possible, set unknowns, delete useless rows)
-- 4. Adjust data types (convert text to INT/DATE where needed)
-- 5. Drop helper columns or unnecessary fields (e.g., row_num) after cleaning
-- ------------------------------------------------------------------------------------------------------------------------
-- ------------------------------------------------------------------------------------------------------------------------




-- ------------------------------------- 1. REMOVING DUPLCATES VALUES -------------------------------------


-- since the table has no unique column, we use ROW_NUMBER() to assign a unique row number for each record 
-- this helps in identifying duplicate rows that share the same values in these fields.
SELECT company, industry, total_laid_off, `date`,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, `date`) AS row_num
FROM layoffs_copy;

-- row_num > 1 implies more than 1 occurences
SELECT * 
FROM (
	SELECT company, industry, total_laid_off, `date`,
	ROW_NUMBER() OVER(
	PARTITION BY company, industry, total_laid_off, `date`) AS row_num
	FROM layoffs_copy)
    duplicates
WHERE row_num > 1;

-- validating the above result
SELECT * FROM layoffs_copy
WHERE company = 'Terminus' OR company = 'Cazoo' OR
	  company = 'Beyond Meat' OR company = 'Oda'
ORDER BY company;

-- we should not proceed like this, as there are some legitimate entries
-- we consider all the columns for partition
-- not considering `source` and `date_added` column as there might be more than one source
-- for the same information posted at different times
SELECT *
FROM (
	SELECT company, location, total_laid_off, `date`, percentage_laid_off, 
    industry, stage, funds_raised, country, 
	ROW_NUMBER() OVER (
	PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, 
    industry, stage, funds_raised, country
	) AS row_num
	FROM layoffs_copy
) duplicates
WHERE row_num > 1;
-- these are the actual duplicate values that we need to remove

-- to remove these values we create another duplicate table with extra column `row_num` (representing occurence of each row)
-- then from this duplicate table we will delete rows which have `row_num` > 1
-- this ensures the dataset is free of duplicates

-- creating a new table `layoffs_copy2` with the same structure as `layoffs_copy`
CREATE TABLE `layoffs_copy2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- inserting all rows from `layoffs_copy` into `layoffs_copy2`
INSERT INTO layoffs_copy2
SELECT *,
ROW_NUMBER() OVER (
	PARTITION BY company, location, industry, total_laid_off,
    percentage_laid_off,`date`, stage, country, funds_raised
	) AS row_num
FROM layoffs_copy;

-- verifying table contents
SELECT * FROM layoffs_copy2;

-- deleting duplicates
DELETE FROM layoffs_copy2
WHERE row_num > 1;

-- verifying above process
SELECT * FROM layoffs_copy2
WHERE row_num > 1;
-- the duplicate entries are now removed






-- ---------------------------------------------------------------------------------------------------
-- ------------------------------------- 2. DATA STANDARDIZATION -------------------------------------
-- ---------------------------------------------------------------------------------------------------



-- table contents
SELECT * FROM layoffs_copy2;

-- we see that in `country` column there are different values for the same country 'United Arab Nations'
SELECT DISTINCT country 
FROM layoffs_copy2
ORDER BY country;

-- let's see more details
SELECT * FROM layoffs_copy2
WHERE country = 'UAE' OR country = 'United Arab Emirates';

-- we can see that most values are 'United Arab Nations', so we will change 'UAE' to 'United Arab Nations'
UPDATE layoffs_copy2
SET country = 'United Arab Emirates'
WHERE country = 'UAE';

-- validating
SELECT DISTINCT country 
FROM layoffs_copy2
ORDER BY country;

-- updating `location` column by removing the extra text ",Non-U.S.", so that only the actual city name remains.
UPDATE layoffs_copy2
SET location = REPLACE(location, ',Non-U.S.', '');

-- validating
SELECT location FROM layoffs_copy2;

-- trimming off extra spaces in `country` column
UPDATE layoffs_copy2
SET company = TRIM(company);

-- validating
SELECT company FROM layoffs_copy2;

-- removing the currency and percentage symbols from `funds_raised` and `percentage_laid_off` columns respectively for future analysis
-- also converting string dates into proper date formats
UPDATE layoffs_copy2
SET funds_raised = REPLACE(funds_raised, '$', ''),
	percentage_laid_off = REPLACE(percentage_laid_off, '%', ''),
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y'),
    `date_added` = STR_TO_DATE(`date_added`, '%m/%d/%Y');

-- renaming column `funds_raised` to `funds_raised_$` for better understanding
-- and modifying the dtypes of the dates columns
ALTER TABLE layoffs_copy2
RENAME COLUMN funds_raised TO funds_raised_$,
MODIFY COLUMN `date` DATE,
MODIFY COLUMN `date_added` DATE;

-- table description
DESC layoffs_copy2;

-- validating modifications made
SELECT * FROM layoffs_copy2;






-- ---------------------------------------------------------------------------------------------------
-- ------------------------------------- 3. MISSING DATA IMPUTATION ----------------------------------
-- ---------------------------------------------------------------------------------------------------



-- table contents
SELECT * FROM layoffs_copy2;

-- replacing blank values with NULL values
UPDATE layoffs_copy2
SET total_laid_off = NULL 
WHERE total_laid_off = '';

-- replacing blank values with NULL values
UPDATE layoffs_copy2
SET percentage_laid_off = NULL 
WHERE percentage_laid_off = '';

-- replacing blank values with NULL values
UPDATE layoffs_copy2
SET funds_raised_$ = NULL 
WHERE funds_raised_$ = '';

-- now changing the dtypes of numerical columns
ALTER TABLE layoffs_copy2
MODIFY COLUMN total_laid_off INT,
MODIFY COLUMN percentage_laid_off INT,
MODIFY COLUMN funds_raised_$ INT;

-- table description
DESC layoffs_copy2;

-- we can see some blank entries in `industry` column
SELECT DISTINCT industry 
FROM layoffs_copy2
ORDER BY industry;

-- more detail on the above blank values
SELECT * FROM layoffs_copy2
WHERE industry LIKE '';

-- more detail on the above blank values
SELECT * FROM layoffs_copy2
WHERE company = 'Eyeo' OR company = 'Appsmith';

-- for companies 'Eyeo' and 'Appsmith', the `industry` column is blank.  
-- since their exact industry is unknown and to maintain consistency,
-- we update the blank values to 'Other' (which already exists as a category
-- in the distinct list of industries).  
UPDATE layoffs_copy2
SET industry = 'Other'
WHERE industry = '';

-- validating
SELECT * FROM layoffs_copy2
WHERE industry LIKE '';

-- we can see some blank entries in `country` column also
SELECT * FROM layoffs_copy2
WHERE country IS NULL
OR country LIKE '';

-- filling in missing `country` values by using information from other rows with the same `location` but a valid `country`.
-- explanation:
-- 		- t1.country = ''  → target rows with blank country values.
--      - t2.country != '' → source rows that have a valid country.
--    the join on `location` ensures we copy the country from a row where the same location is already mapped correctly.
UPDATE layoffs_copy2 t1
JOIN layoffs_copy2 t2
  ON t1.location = t2.location
SET t1.country = t2.country
WHERE t1.country = ''   -- blank country values
  AND t2.country != ''; -- matching row with a valid country

-- validating
SELECT * FROM layoffs_copy2
WHERE country IS NULL
OR country LIKE '';

-- blank values in `stage` column also
SELECT distinct stage
FROM layoffs_copy2;

-- more information
select * from layoffs_copy2
where stage = '';

-- updating the blank values to 'Unknown' (already existing category)
UPDATE layoffs_copy2
SET stage = 'Unknown'
WHERE stage = '';

-- validating
SELECT distinct stage
FROM layoffs_copy2;

-- NULL values which are meaningless
SELECT *
FROM layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- deleting useless data we can't really use
DELETE FROM layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- dropping `row_num` column initially created
ALTER TABLE layoffs_copy2
DROP COLUMN row_num;

-- final clean dataset
SELECT * FROM layoffs_copy2;