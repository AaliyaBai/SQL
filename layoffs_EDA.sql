select * from layoffs_staging2;

select max(total_laid_off) , max(percentage_laid_off) 
from layoffs_staging2;

select *  
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select *  
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select min(`date`), max(`date`)
from layoffs_staging2
;

select min(`date`), max(`date`)
from layoffs_staging2
;

select company, max(total_laid_off) , max(percentage_laid_off) 
from layoffs_staging2
group by company
order by 2
 ;

select industry , sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country , sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`) , sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc;

select stage , sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select company , avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;


select substring(`date`,6,2) as month
from layoffs_staging2;

select year(`date`) as month, sum(total_laid_off)
from layoffs_staging2
where year(`date`) is not null
group by year(`date`)
order by 1 asc;

select * from layoffs_staging2;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

select year(`date`) as `year`, sum(total_laid_off)
from layoffs_staging2
where year(`date`) is not null
group by `year` 
order by `year` desc ;

select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_laid
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

with rolling_total as
( select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_laid
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_laid, sum(total_laid) over(order by `month`) as rolling_total
from rolling_total ;


select company, year(`date`), sum(total_laid_off) as total_laid
from layoffs_staging2
where year(`date`) is not null
group by year(`date`), company
order by 3 desc
;


with Company_Year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off) as total_laid
from layoffs_staging2
where year(`date`) is not null
group by year(`date`), company
order by 3 desc
),
company_ranks as
(
select *, 
dense_rank() over (partition by years order by total_laid_off desc) as ranks
from Company_Year
)
select * 
from company_ranks
where ranks <= 5
;





with rolling_total_month as 
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) 
over(partition by substring(`date`,1,7) order by industry ) as rolling_total
from layoffs_staging2
group by company, (substring(`date`,1,7)) )

select  distinct(`month`) as RollingTotalMonth
from rolling_total_month ;





select industry, total_laid_off, (substring(`date`,1,7)) as `month`, sum(total_laid_off) 
over(partition by substring(`date`,1,7) order by industry ) as rolling_total
from layoffs_staging2;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
over(order by substring(`date`,1,7)) as rolling_total
from layoffs_staging2  ;


















SELECT 
   `date` , 
    SUM(total_laid_off) OVER (PARTITION BY (`date`)) AS rolling_total
FROM layoffs_staging2;
