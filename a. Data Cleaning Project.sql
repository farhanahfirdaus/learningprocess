-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Satrt with, recreating raw data table, so that we have back up to use if we delete some data later
-- 2. Identify and remove Duplicates
-- 3. Standardize the Data
-- 4. Null or blank values
-- 5. Remove unnecessary columns or rows


-- 1. Satrt with, recreating raw data table, so that we have back up to use if we delete some data later

CREATE TABLE layodffs_staging
LIKE layoffs;

SELECT *
FROM layodffs_staging;

INSERT layodffs_staging
SELECT * 
FROM layoffs;


-- 2. Identify and remove Duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layodffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layodffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layodffs_staging
WHERE Company = 'Casper'
;



CREATE TABLE `layodffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layodffs_staging2
WHERE row_num > 1;

INSERT INTO layodffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layodffs_staging;


DELETE
FROM layodffs_staging2
WHERE row_num > 1;


SELECT *
FROM layodffs_staging2;

-- 3. Standardize the Data

SELECT company, TRIM(company)
FROM layodffs_staging2;

UPDATE layodffs_staging2
SET company = TRIM(company);

SELECT *
FROM layodffs_staging2
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layodffs_staging2;

UPDATE layodffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 

SELECT *
FROM layodffs_staging2
WHERE country LIKE 'United State%'
ORDER BY 1
;

SELECT DISTINCT country, TRIM(country)
FROM layodffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layodffs_staging2
ORDER BY 1;

UPDATE layodffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layodffs_staging2;

UPDATE layodffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layodffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layodffs_staging2;

-- 4. Null or blank values

SELECT *
FROM layodffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

UPDATE layodffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT DISTINCT industry
FROM layodffs_staging2;

SELECT *
FROM layodffs_staging2
WHERE industry IS NULL
OR industry = ''
;


SELECT *
FROM layodffs_staging2
WHERE company LIKE 'Bally%';

SELECT t1.industry, t2.industry
FROM layodffs_staging2 t1
JOIN layodffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL
;

UPDATE layodffs_staging2 t1
JOIN layodffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

-- 5. Remove unnecessary columns or rows
-- Only if you confident the data are unnecessary/useless

SELECT *
FROM layodffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layodffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layodffs_staging2;

ALTER TABLE layodffs_staging2
DROP COLUMN row_num;