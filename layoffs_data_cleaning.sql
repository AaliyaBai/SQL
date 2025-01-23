---- 1 remove duplicates
---  2 standardize the Data 
---  3 check for Null values or blank values
---- 4 remove any columns that are irrelevant

--- creating a staging table ----

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select *
from layoffs; 

--- 1) checking duplicate rows


with duplicate_cte as
( select *,
row_number() over(partition by company, location, 
								industry, total_laid_off, percentage_laid_off, 
                                `date`,	stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

--- as cte cannot be updatable, we cannot delete the row_num from cte ---
with duplicate_cte as
( select *,
row_number() over(partition by company, location, 
								industry, total_laid_off, percentage_laid_off, 
                                `date`,	stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

---- Hence we will be creating another layoff_staging2 and insert all the rows into the new table and delete the duplicate rows ---

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` double DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(partition by company, location, 
								industry, total_laid_off, percentage_laid_off, 
                                `date`,	stage, country, funds_raised_millions) as row_num
 from layoffs_staging;
 
select * from layoffs_staging2
where row_num > 1;

--- deleting the row_num duplcates now ---

SET SQL_SAFE_UPDATES = 0;

delete 
from layoffs_staging2
where row_num > 1;

SET SQL_SAFE_UPDATES = 1;


--- 2) standardize the Data 


select company, trim(company)  from layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;
update layoffs_staging2
set company = trim(company)
;
SET SQL_SAFE_UPDATES = 1;

select distinct(country) from layoffs_staging2
order by 1;

select distinct(country), trim(trailing '.' from country) 
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country) 
where country like 'United States%';

select distinct(industry) from layoffs_staging2
order by 1;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%' ;

select * from layoffs_staging2;

--- updating the `date` variable from text to string variable ---

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;

--- 3) Null and blank values ---


select *
from layoffs_staging2;

select *
from layoffs_staging2
where industry is null
or  industry = '' ;

select * 
from layoffs_staging2
where company = 'bally%';

update layoffs_staging2
set industry = null
where industry = '';
 
select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
		on t1.company = t2.company
        where (t1.industry is null
			or  t1.industry = '') and t2.industry is not null ;

update layoffs_staging2 t1
join layoffs_staging2 t2
		on t1.company = t2.company
	set t1.industry = t2.industry
    where t1.industry is null
			and t2.industry is not null ;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null ;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null ;

select * 
from layoffs_staging2;

--- 4) dropping the unwanted column ---

alter table layoffs_staging2
drop column row_num;

SET SQL_SAFE_UPDATES = 1;



