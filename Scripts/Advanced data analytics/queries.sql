-- Change over time Analysis

SELECT 
year(ordeR_date) as order_year,
month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(ordeR_date),month(order_date)
Order by year(ordeR_date),month(order_date);
