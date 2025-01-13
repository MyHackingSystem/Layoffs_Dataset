-- @Zakariae el kadiri
-- "Layoffs Dataset Analytics" project, I focused on transforming raw data into actionable insights by applying data cleaning and exploration techniques. This project highlights my ability to work with real-world datasets, ensuring data quality and consistency for accurate analysis.
-- Dataset source: https://www.kaggle.com/datasets/theakhilb/layoffs-data-2022

-- Data Cleaning

SELECT * 
FROM layoffs_dataset_2024;

-- Create staging data to work on

CREATE TABLE layoffs_staging_1 AS
SELECT * FROM layoffs_dataset_2024;

-- Remove Duplicate Records

SELECT *,
RoW_NUMBER() OVER(PARTITION BY Company, Location_HQ, Industry, Laid_Off_Count, Funds_Raised) AS Row_Num
FROM layoffs_staging_1;

WITH duplicates_cte AS 
(
SELECT *,
RoW_NUMBER() OVER(PARTITION BY Company, Location_HQ, Industry, Laid_Off_Count, `Date`, Funds_Raised, Stage, Country, Percentage) AS Row_Num
FROM layoffs_staging_1
)
SELECT * 
FROM duplicates_cte
WHERE Row_Num > 1;

SELECT * FROM layoffs_staging_1 WHERE Company = "Beyond Meat";

WITH duplicates_cte AS 
(
SELECT *,
RoW_NUMBER() OVER(PARTITION BY Company, Location_HQ, Industry, Laid_Off_Count, `Date`, Funds_Raised, Stage, Country, Percentage) AS Row_Num
FROM layoffs_staging_1
)
SELECT * 
FROM duplicates_cte
WHERE Row_Num > 1;

SELECT * FROM layoffs_staging_1;


CREATE TABLE `layoffs_staging_2` (
  `Company` text,
  `Location_HQ` text,
  `Industry` text,
  `Laid_Off_Count` text,
  `Date` text,
  `Source` text,
  `Funds_Raised` double DEFAULT NULL,
  `Stage` text,
  `Date_Added` text,
  `Country` text,
  `Percentage` text,
  `List_of_Employees_Laid_Off` text,
  `Row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT * FROM layoffs_staging_2;

INSERT INTO layoffs_staging_2
SELECT *,
RoW_NUMBER() OVER(PARTITION BY Company, Location_HQ, Industry, Laid_Off_Count, `Date`, Funds_Raised, Stage, Country, Percentage) AS Row_Num
FROM layoffs_staging_1;

SELECT * FROM layoffs_staging_2;

SELECT * 
FROM layoffs_staging_2
WHERE Row_num > 1;

DELETE
FROM layoffs_staging_2
WHERE Row_num > 1;

SELECT * 
FROM layoffs_staging_2
WHERE Row_num > 1;

-- Standardize the data

SELECT * 
FROM layoffs_staging_2;

SELECT Company, TRIM(Company)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET Company = TRIM(Company);

SELECT DISTINCT Location_HQ
FROM layoffs_staging_2
ORDER BY 1;

SELECT * 
FROM layoffs_staging_2
WHERE Location_HQ LIKE '%sseldorf';

UPDATE layoffs_staging_2
SET Location_HQ = 'Dusseldorf'
WHERE Location_HQ LIKE '%sseldorf';

SELECT * 
FROM layoffs_staging_2
WHERE Location_HQ LIKE 'Malm%';

UPDATE layoffs_staging_2
SET Location_HQ = 'Malmo'
WHERE Location_HQ LIKE 'Malm%';

SELECT Location_HQ, TRIM(Location_HQ)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET Location_HQ = TRIM(Location_HQ);

SELECT DISTINCT Industry
FROM layoffs_staging_2
ORDER BY 1;

SELECT Industry, TRIM(Industry)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET Industry = TRIM(Industry);

SELECT DISTINCT Stage
FROM layoffs_staging_2
ORDER BY 1;

SELECT Stage, TRIM(Stage)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET Stage = TRIM(Stage);

SELECT DISTINCT Country
FROM layoffs_staging_2
ORDER BY 1;

SELECT Country, TRIM(Country)
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET Country = TRIM(Country);

SELECT * 
FROM layoffs_staging_2;

-- backup data before alter table

CREATE TABLE layoffs_staging_3 AS
SELECT * FROM layoffs_staging_2;

SELECT * 
FROM layoffs_staging_3;

SELECT `date`
FROM layoffs_staging_3;

ALTER TABLE layoffs_staging_3
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging_3;

SELECT `Date_Added`
FROM layoffs_staging_3;

ALTER TABLE layoffs_staging_3
MODIFY COLUMN `Date_Added` DATETIME;

SELECT * 
FROM layoffs_staging_3;

SELECT `Laid_Off_Count`
FROM layoffs_staging_3;


CREATE TABLE layoffs_staging_4 AS
SELECT * FROM layoffs_staging_3;

SELECT * 
FROM layoffs_staging_4;

-- Check if there are any non-numeric values.

SELECT *
FROM layoffs_staging_4
WHERE `Laid_Off_Count` IS NOT NULL;

SELECT *
FROM layoffs_staging_4
WHERE `Laid_Off_Count` IS NOT NULL AND `Laid_Off_Count` != '';

SELECT *
FROM layoffs_staging_4
WHERE `Laid_Off_Count` REGEXP '^[0-9]+$';

SELECT *
FROM layoffs_staging_4
WHERE `Laid_Off_Count` NOT REGEXP '^[0-9]+$';

SELECT *
FROM layoffs_staging_4
WHERE `Laid_Off_Count` IS NOT NULL AND `Laid_Off_Count` != '' AND `Laid_Off_Count` NOT REGEXP '^[0-9]+$';

SELECT *
FROM layoffs_staging_4
WHERE `Laid_Off_Count` IS NULL OR `Laid_Off_Count` = '';

UPDATE layoffs_staging_4
SET `Laid_Off_Count` = NULL
WHERE `Laid_Off_Count` IS NULL OR `Laid_Off_Count` = '';

ALTER TABLE layoffs_staging_4
MODIFY COLUMN `Laid_Off_Count` INT;

SELECT * 
FROM layoffs_staging_4;

CREATE TABLE layoffs_staging_5 AS
SELECT * FROM layoffs_staging_4;

SELECT * 
FROM layoffs_staging_5;

SELECT Percentage
FROM layoffs_staging_5;

SELECT *
FROM layoffs_staging_5
WHERE Percentage IS NOT NULL;

SELECT *
FROM layoffs_staging_5
WHERE Percentage IS NULL OR Percentage = '';

SELECT *
FROM layoffs_staging_5
WHERE Percentage = '';

UPDATE layoffs_staging_5
SET Percentage = NULL
WHERE Percentage = '';

SELECT *
FROM layoffs_staging_5
WHERE Percentage = '';

CREATE TABLE layoffs_staging_6 AS
SELECT * FROM layoffs_staging_5;

SELECT *
FROM layoffs_staging_6;

ALTER TABLE layoffs_staging_6
MODIFY COLUMN Percentage DOUBLE;

SELECT *
FROM layoffs_staging_6;

-- Handle missing data (null values or blank values)

SELECT * 
FROM layoffs_staging_6;

SELECT * 
FROM layoffs_staging_6
WHERE Company IS NULL 
OR Company = '';

SELECT * 
FROM layoffs_staging_6
WHERE Location_HQ IS NULL 
OR Location_HQ = '';

SELECT * 
FROM layoffs_staging_6
WHERE Industry IS NULL 
OR Industry = '';

SELECT *
FROM layoffs_staging_6
WHERE Laid_Off_Count IS NULL OR Laid_Off_Count = '';

SELECT *
FROM layoffs_staging_6
WHERE (Laid_Off_Count IS NULL OR Laid_Off_Count = '')
AND (Funds_Raised IS NULL OR Funds_Raised = '')
AND (Percentage IS NULL OR Percentage = ''); 

DELETE
FROM layoffs_staging_6
WHERE (Laid_Off_Count IS NULL OR Laid_Off_Count = '')
AND (Funds_Raised IS NULL OR Funds_Raised = '')
AND (Percentage IS NULL OR Percentage = ''); 

SELECT *
FROM layoffs_staging_6
WHERE (Laid_Off_Count IS NULL OR Laid_Off_Count = '')
AND (Percentage IS NULL OR Percentage = ''); 

CREATE TABLE layoffs_staging_with_null AS
SELECT * FROM layoffs_staging_6;

SELECT *
FROM layoffs_staging_with_null;

CREATE TABLE layoffs_staging_7 AS
SELECT * FROM layoffs_staging_6;

SELECT *
FROM layoffs_staging_7;

SELECT *
FROM layoffs_staging_7
WHERE (Laid_Off_Count IS NULL OR Laid_Off_Count = '')
AND (Percentage IS NULL OR Percentage = ''); 

DELETE
FROM layoffs_staging_7
WHERE (Laid_Off_Count IS NULL OR Laid_Off_Count = '')
AND (Percentage IS NULL OR Percentage = '');

SELECT *
FROM layoffs_staging_7;

-- Remove Unnecessary column

ALTER TABLE layoffs_staging_7
DROP COLUMN Row_num;

SELECT * 
FROM layoffs_staging_7;

CREATE TABLE layoffs_staging_8 AS
SELECT * FROM layoffs_staging_7;

SELECT * 
FROM layoffs_staging_8;

ALTER TABLE layoffs_staging_8
DROP COLUMN List_of_Employees_Laid_Off;

SELECT * 
FROM layoffs_staging_8;


ALTER TABLE layoffs_staging_8
DROP COLUMN `Source`;

SELECT * 
FROM layoffs_staging_8;

-- Exploratory Data Analysis

SELECT * 
FROM layoffs_staging_8;

SELECT MAX(Laid_Off_Count), MAX(Percentage), MAX(Funds_Raised)
FROM layoffs_staging_8;

SELECT MIN(Laid_Off_Count), MIN(Percentage), MIN(Funds_Raised)
FROM layoffs_staging_8;

SELECT * 
FROM layoffs_staging_8
WHERE Percentage = 1
ORDER BY Funds_Raised DESC;


SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging_8;

SELECT Company, SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY Company
ORDER BY 2 DESC;

SELECT Industry, SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY Industry
ORDER BY 2 DESC;

SELECT Location_HQ, SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY Location_HQ
ORDER BY 2 DESC;

SELECT Country, SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY Country
ORDER BY 2 DESC;

SELECT * 
FROM layoffs_staging_8;

SELECT Company, SUM(Funds_Raised)
FROM layoffs_staging_8
GROUP BY Company
ORDER BY 2 DESC;

SELECT Industry, SUM(Funds_Raised)
FROM layoffs_staging_8
GROUP BY Industry
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT YEAR(`date`), SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT Stage, SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY Stage
ORDER BY 2 DESC;
 

SELECT * 
FROM layoffs_staging_8;

SELECT SUBSTRING(`Date`,1,7) AS `MONTH`, SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY `MONTH`
ORDER BY 1 ASC;


SELECT * 
FROM layoffs_staging_8;

SELECT SUBSTRING(`Date`,1,7) AS `MONTH`, SUM(Laid_Off_Count)
FROM layoffs_staging_8
GROUP BY `MONTH`
ORDER BY 1 ASC;


WITH Rolling_Laid_Off AS
(
SELECT SUBSTRING(`Date`,1,7) AS `MONTH`, SUM(Laid_Off_Count) AS Laid_Off
FROM layoffs_staging_8
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, Laid_Off,
SUM(Laid_Off) OVER(ORDER BY `MONTH`) AS Rolling_Total
FROM Rolling_Laid_Off;

SELECT Company, YEAR(`Date`), SUM(Funds_Raised)
FROM layoffs_staging_8
GROUP BY Company, YEAR(`Date`)
ORDER BY 3 DESC;

WITH Company_Year(company, years, total_laid_off) AS
(
SELECT Company, YEAR(`Date`), SUM(Funds_Raised)
FROM layoffs_staging_8
GROUP BY Company, YEAR(`Date`)
), Company_Rank_Year AS
(
seLECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
)
SELECT * 
FROM Company_Rank_Year
WHERE Ranking <= 5;




















