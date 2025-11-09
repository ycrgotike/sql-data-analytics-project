--1. Change over time Trends

--Analyse sales performance over time
SELECT
YEAR(order_date) AS order_year,
SUM(sales_amount) total_sales,
COUNT(DISTINCT customer_key) total_customers,
SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

-- Analyse monthly sales performance
SELECT
MONTH(order_date) AS order_month,
SUM(sales_amount) total_sales,
COUNT(DISTINCT customer_key) total_customers,
SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date)

-- Analyse sales performance by year and month
SELECT
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) total_sales,
COUNT(DISTINCT customer_key) total_customers,
SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)

--This needs CLR enabled on the SQL Server instance to use DATERUNC function
SELECT
DATETRUNC(MONTH, order_date) AS order_month_start,
SUM(sales_amount) total_sales,
COUNT(DISTINCT customer_key) total_customers,
SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date)

--This needs CLR enabled on the SQL Server instance to use FORMAT function
SELECT
FORMAT(order_date, 'yyyy-MMM') AS order_month_start,
SUM(sales_amount) total_sales,
COUNT(DISTINCT customer_key) total_customers,
SUM(quantity) total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM')

--2.Cumulative Trends and analysis - aggregate progressively over time
--[cumulative measure] by [date dimension] like 
--running total sales by year and month; moving average sales by year and month

--Calculate total sales per month and running total sales over time
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
AVG(avg_price) OVER (ORDER BY order_date) AS moving_avg_sales
FROM (
SELECT
DATEADD(YEAR, DATEDIFF(YEAR, 0, order_date), 0) AS order_date,
--DATETRUNC(MONTH, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(Price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
--GROUP BY DATETRUNC(MONTH, order_date)
GROUP BY DATEADD(YEAR, DATEDIFF(YEAR, 0, order_date), 0) 
) AS monthly_sales

--3.Performance Analysis - compare current value to a target value
--can be done by Current[measure] - Target[Measure], current sales - avg sales;
--current year sales - previous year sales(YoY)
--current sales - lowest sales 

/*Analyse yearly performance of products by comparing their sales to both
average sales performance of the product and previous year's sales*/

WITH yearly_product_sales AS (  
SELECT
YEAR(f.order_date) AS order_year,   
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY
YEAR(f.order_date),
p.product_name
)
SELECT
order_year,
product_name,   
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) AS avg_product_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS sales_vs_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) >= 0 THEN 'Above Average' 
    WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average' 
    ELSE 'Average'
END AS avgchange,
-- Year over Year comparison
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS sales_vs_previous_year,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increased' 
    WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decreased' 
    ELSE 'No Change'
END AS yoy_change
FROM yearly_product_sales
ORDER BY product_name, order_year

--4.Part to whole Analysis - compare part to whole
--measure/total measure * 100 by dimension
--sales/total sales * 100 by product category

--which category contributes how much to overall sales
WITH category_sales AS (
SELECT  
category,
SUM(sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key    
WHERE f.order_date IS NOT NULL
GROUP BY p.category
)
SELECT
category,
total_sales,
SUM(total_sales) OVER () AS overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2), '%') AS pct_of_total_sales
FROM category_sales
ORDER BY total_sales DESC

--5.Data Segmentation Analysis - compare segments within a dimension
--group data based on specific range
--Measure / Measure; total products/sales range; total customers/age group

/*segment products into cost ranges and 
count how many products fall into each range */

WITH product_cost_segments AS (
SELECT
product_key,
product_name,
cost,
CASE 
    WHEN cost < 100 THEN 'Low Cost (<100)'
    WHEN cost >= 100 AND cost < 500 THEN 'Medium Cost (100-500)'
    WHEN cost Between 500 AND 1000 THEN 'High Cost (500-1000)'
    ELSE 'High Cost (>1000)'
END AS cost_segment
FROM gold.dim_products
)
SELECT
cost_segment,   
COUNT(product_key) AS total_products,
SUM(cost) AS total_cost
FROM product_cost_segments
GROUP BY cost_segment
ORDER BY total_products DESC

/*Group customers into 3 segments based on their spending behavior
1.VIP: atleast 12 months of history and spending more than 5000 euros
2.Regular: atleast 12 months of history and spending less than 5000 euros
3.New: with a lifespan less than 12 months
and find total customers by each segment
*/


WITH customer_spending AS (
SELECT
c.customer_key,
SUM(f.sales_amount) AS total_spent,
MIN(f.order_date) AS first_order,
MAX(f.order_date) AS last_order,
DATEDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) AS customer_lifespan_months
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)

SELECT
customer_segment,
COUNT(customer_key) AS total_customers
FROM (
SELECT
customer_key,   
total_spent,    
customer_lifespan_months,
CASE 
    WHEN customer_lifespan_months >= 12 AND total_spent > 5000 THEN 'VIP'
    WHEN customer_lifespan_months >= 12 AND total_spent <= 5000 THEN 'Regular'
    WHEN customer_lifespan_months < 12 THEN 'New'
END AS customer_segment
FROM customer_spending) t 
GROUP BY customer_segment
ORDER BY total_customers DESC

--6.Build Customer Reports
