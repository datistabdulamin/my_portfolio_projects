-- datistabdulamin Portfolio Project
-- Project: Data Cleaning
-- Data: From Kaggle

-- checking data
SELECT * FROM layoffs;

-- Make demi table
CREATE TABLE layoffs_staging LIKE layoffs;
SELECT * FROM layoffs_staging;

-- insert data to this table from existing table
INSERT layoffs_staging
SELECT * from layoffs;

SELECT * FROM layoffs_staging;

-- 1. Remover The Duplicates values if any
-- checking for duplicate

WITH duplicate_cte AS
(
SELECT *,
row_number() OVER(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
FROM layoffs_staging
)
SELECT * FROM duplicate_cte
where row_num >1;

-- delete the duplicate
WITH duplicate_cte AS
(
SELECT *,
row_number() OVER(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
where row_num >1;

-- 2. Standardize Data

SELECT * 
FROM layoffs_staging;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY industry;

SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM layoffs_staging
WHERE company LIKE 'Bally%';
-- nothing wrong here
SELECT *
FROM layoffs_staging
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_staging
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging t1
JOIN layoffs_staging t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY industry;

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM layoffs_staging;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM layoffs_staging
ORDER BY country;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM layoffs_staging
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM layoffs_staging;

-- we can use str to date to update this field
UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging;





-- 3. Look at Null Values
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging;

ALTER TABLE layoffs_staging
DROP COLUMN row_num;


SELECT * 
FROM layoffs_staging;

-- the end

-- #datistabdulamin


