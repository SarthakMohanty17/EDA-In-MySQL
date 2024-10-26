
SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize The Data
-- 3. Looking for null values
-- 4. Remove unncessary columns or rows

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date') AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging_2` (
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
FROM layoffs_staging_2;

INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging_2
WHERE row_num > 1;


SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoffs_staging_2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging_2;

-- Standardizing Data

SELECT company , TRIM(COMPANY)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging_2;

SELECT *
FROM layoffs_staging_2
WHERE industry LIKE '%Crypto%';

UPDATE layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';

SELECT *
FROM layoffs_staging_2
WHERE industry = 'Crypto';

SELECT DISTINCT industry
FROM layoffs_staging_2;

SELECT DISTINCT country
FROM layoffs_staging_2
ORDER BY 1;

UPDATE layoffs_staging_2
SET country = 'United States'
WHERE country LIKE '%United States%' ;

SELECT 'date'
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET date = STR_TO_DATE( 'date' , '%m/%d/%Y');

SELECT date
FROM layoffs_staging_2;

ALTER TABLE layoffs_staging_2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging_2
WHERE industry = ''
OR industry IS NULL;

UPDATE layoffs_staging_2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry , t2.industry
FROM layoffs_staging_2 t1
INNER JOIN layoffs_staging_2 t2
ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging_2 t1
INNER JOIN layoffs_staging_2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL;

SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging_2;

-- Exploratory Data Analysis

SELECT MAX(total_laid_off) , MAX(percentage_laid_off)
FROM layoffs_staging_2;

SELECT *
FROM layoffs_staging_2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging_2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;

SELECT MIN(`date`) , MAX(`date`)
FROM layoffs_staging_2;

SELECT industry , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`) , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY `MONTH`
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging_2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 DESC;


 WITH Rolling_total AS
 (
 SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging_2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 DESC
)
SELECT `MONTH` ,total_off , SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_total;


SELECT company , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company
ORDER BY 2 DESC;

SELECT company , YEAR(`date`) , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC ;

WITH Company_year(company , Years , Total_laid_off) AS
(
SELECT company , YEAR(`date`) , SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company , YEAR(`date`)
), Company_year_rank AS
(
SELECT *, DENSE_RANK() OVER (PARTITION BY Years ORDER BY Total_laid_off DESC) AS Ranking
FROM Company_year
WHERE Years IS NOT NULL
ORDER BY Ranking ASC
)
SELECT *
FROM Company_year_rank
WHERE Ranking <= 5
ORDER BY Years;












 
 
 





























