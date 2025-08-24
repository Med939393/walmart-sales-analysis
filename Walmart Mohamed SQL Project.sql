SELECT invoice_id , count(*) FROM walmart group by 1 having count(*) > 1 ;

select * from walmart where invoice_id in ('9950' , '9951') ; 

with cte as ( select *,row_number() over (partition by invoice_id order by `date` ) as rn from walmart)
select * from cte where rn > 1 ;


WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY `date`) AS rn
    FROM walmart
)
DELETE FROM walmart
WHERE (invoice_id, `date`, payment_method) IN (
    SELECT invoice_id, `date`, payment_method FROM cte WHERE rn > 1
);

select * from walmart ;


UPDATE walmart
SET converted = STR_TO_DATE(`date`, '%d/%m/%y')
WHERE STR_TO_DATE(`date`, '%d/%m/%y') IS NOT NULL;
select * from walmart ;

ALTER TABLE walmart
DROP COLUMN date_of_order,
DROP COLUMN `date`;

ALTER TABLE walmart
CHANGE COLUMN converted order_date DATE;



SELECT unit_price
FROM walmart
WHERE unit_price IS NOT NULL
  AND CAST(REPLACE(REPLACE(TRIM(unit_price), '$', ''), ',', '') AS DECIMAL(10,2)) IS NULL;

UPDATE walmart
SET unit_price = TRIM(REPLACE(REPLACE(unit_price, '$', ''), ',', ''))
WHERE unit_price IS NOT NULL;



WITH cte AS (
    SELECT city,
        category,payment_method,
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date ) AS month,
        round(SUM(quantity * unit_price),2) AS total_revenue,
        COUNT(DISTINCT invoice_id) AS total_orders
    FROM walmart
    GROUP BY city,category,payment_method, year, month
)
SELECT 
    year,total_revenue,
    round(sum(total_revenue) over (order by year),2) as moving_total_revenue,
    lag(total_revenue) over (partition by year order by total_revenue) as previous_revenue
FROM cte ; 

with cte as ( 
select EXTRACT(YEAR FROM order_date) AS year,round(SUM(quantity * unit_price),2) AS total_revenue from walmart group by 1 ) , cte1 as (
SELECT 
    year,total_revenue,
    round(sum(total_revenue) over (order by year),2) as moving_total_revenue,
    lag(total_revenue) over (order by year ) as previous_revenue,
    total_revenue - lag(total_revenue) over (order by year ) as differance
FROM cte )

select year,total_revenue,moving_total_revenue,previous_revenue,differance,

case when differance < 0 then 'Decrease'
     when differance > 0 then 'Increase'
     else  'No change' 
END as Segment_product    from cte1 ;




with cte as ( 
select EXTRACT(MONTH FROM order_date ) AS month,city,
        category,payment_method,

round(SUM(quantity * unit_price),2) AS total_revenue from walmart group by 1,2,3,4 ) , cte1 as (
SELECT 
    month,city,
        category,payment_method,
    
    total_revenue,
    round(sum(total_revenue) over (order by month),2) as moving_total_revenue,
    lag(total_revenue) over (order by month ) as previous_revenue,
    ROUND(total_revenue - lag(total_revenue) over (order by month ),0) as differance
FROM cte )

select month,city,
        category,payment_method,
total_revenue,moving_total_revenue,previous_revenue,differance,

case when differance < 0 then 'Decrease'
     when differance > 0 then 'Increase'
     else  'No change' 
END as Segment_product    from cte1 ;








WITH cte AS ( 
    SELECT 
        EXTRACT(MONTH FROM order_date) AS month,
        city,
        category,
        payment_method,
        ROUND(SUM(quantity * unit_price), 2) AS total_revenue
    FROM walmart
    GROUP BY 1, 2, 3, 4
),
cte1 AS (
    SELECT 
        month,
        city,
        category,
        payment_method,
        total_revenue,
        ROUND(SUM(total_revenue) OVER (ORDER BY month), 2) AS moving_total_revenue,
        LAG(total_revenue) OVER (ORDER BY month) AS previous_revenue,
        ROUND(total_revenue - LAG(total_revenue) OVER (ORDER BY month), 0) AS difference,
        DENSE_RANK() OVER (PARTITION BY month ORDER BY total_revenue DESC) AS city_rank
    FROM cte
)
SELECT 
    month,
    city,
    category,
    payment_method,
    total_revenue,
    moving_total_revenue,
    previous_revenue,
    difference,
    city_rank,
    CASE 
        WHEN difference < 0 THEN 'Decrease'
        WHEN difference > 0 THEN 'Increase'
        ELSE 'No change' 
    END AS Segment_product
FROM cte1
ORDER BY month, city_rank ;




WITH cte AS ( 
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
        ROUND(AVG(profit_margin), 2) AS avg_profit_margin
    FROM walmart
    GROUP BY 1
),
cte1 AS (
    SELECT 
        year,
        total_revenue,
        avg_profit_margin,
        ROUND(SUM(total_revenue) OVER (ORDER BY year), 2) AS moving_total_revenue,
        LAG(total_revenue) OVER (ORDER BY year) AS previous_revenue,
        ROUND(total_revenue - LAG(total_revenue) OVER (ORDER BY year), 2) AS difference,
        LAG(avg_profit_margin) OVER (ORDER BY year) AS previous_margin,
        ROUND(avg_profit_margin - LAG(avg_profit_margin) OVER (ORDER BY year), 2) AS margin_change
    FROM cte
)
SELECT 
    year,
    total_revenue,
    avg_profit_margin,
    moving_total_revenue,
    previous_revenue,
    difference,
    previous_margin,
    margin_change,
    CASE 
        WHEN difference < 0 THEN 'Decrease'
        WHEN difference > 0 THEN 'Increase'
        ELSE 'No change' 
    END AS Segment_product,
    CASE 
        WHEN margin_change IS NULL THEN 'First Year'
        WHEN margin_change > 0 THEN 'Improved Margin'
        WHEN margin_change < 0 THEN 'Declined Margin'
        ELSE 'Stable Margin'
    END AS Margin_Trend
FROM cte1
ORDER BY year;





