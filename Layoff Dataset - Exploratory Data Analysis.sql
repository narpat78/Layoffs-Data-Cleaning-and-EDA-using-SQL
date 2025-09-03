-- -------------------------------------------------------------------------------------------------------------------
--                                                EXPLORATORY DATA ANALYSIS
-- -------------------------------------------------------------------------------------------------------------------

-- select the database
USE world_layoffs;
-- check the tables present
SHOW TABLES;
-- our cleaned dataset for analysis
SELECT * FROM layoffs_copy2;


-- DATE RANGE OF OUR DATASET
SELECT MAX(`date`),  MIN(`date`)
FROM layoffs_copy2;
-- from 11/03/2020 till 23/08/2025 


-- maximum layoff on a single day
SELECT MAX(total_laid_off)
FROM layoffs_copy2;
-- 22000 layoffs in a single day


-- look at percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_copy2
WHERE  percentage_laid_off IS NOT NULL;
-- 100 and 0 (as expected)


-- companies which had 100% layoff
SELECT *
FROM layoffs_copy2
WHERE  percentage_laid_off = 100;
-- total of 328 companies which got completely laid off


-- order by funcs_raised_$ (these are in million dollars) to see how big some of these companies were
SELECT *
FROM layoffs_copy2
WHERE percentage_laid_off = 100
ORDER BY funds_raised_$ DESC;
-- Britishvolt, Quibi, Fisker, and many more companies raising billion dollars and getting laid off completely


-- companies with the biggest single Layoff
SELECT company, total_laid_off
FROM layoffs_copy2
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day
-- Intel with 37000, Tesla 14000, Google 12000, and Meta 11000
-- some big names


-- companies with the most total layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;
-- Intel, Microsoft and Amazon leading the chart with most layoffs 2020 - 2025


-- total layoffs by location
SELECT location, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;


-- total layoffs by country
SELECT country, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY country
ORDER BY 2 DESC;
-- United States with more than half a million
-- followed by India and Germany


-- total layoffs per year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY YEAR(`date`)
ORDER BY 1 ASC;


-- total layoffs by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY industry
ORDER BY 2 DESC;
-- Hardware industry affected the most


-- total layoffs by company stage
SELECT stage, SUM(total_laid_off)
FROM layoffs_copy2
GROUP BY stage
ORDER BY 2 DESC;
-- most companies are laying off employees after IPO 


-- -------------------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------------------


-- earlier we looked at companies with the most layoffs
-- now let's look at that per year
-- finding the top 3 companies with the highest layoffs for each year

-- aggregating the total layoffs per company per year
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_copy2
  GROUP BY company, YEAR(date)
)
-- ranking companies within each year by their total layoffs (highest first)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
-- selecting only the top 3 companies per year
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;



-- -------------------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------------------



-- calculating the rolling total of layoffs per month
-- aggregating layoffs per month (using first 7 chars YYYY-MM format from date)
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_copy2
GROUP BY dates
ORDER BY dates ASC;

-- now using the monthly totals in a CTE for further calculations
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_copy2
GROUP BY dates
ORDER BY dates ASC
)
-- calculating the rolling (cumulative) total of layoffs over time
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;