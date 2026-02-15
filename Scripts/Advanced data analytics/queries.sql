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



-----performance analysis
with yearly_sales as 
(
 select
 year(f.order_date) as order_year,
 p.product_name as product_name,
 sum(f.sales_amount) as current_sales
 from gold.fact_sales f
 left join gold.dim_products p
 on f.product_key=p.product_key
 where f.order_date is not null
 group by year(f.order_date),
 p.product_name
 )
 select
 order_year,
 product_name,
 current_sales,
 avg(current_sales) over(partition by product_name) as avg_sales,
 current_sales-avg(current_sales) over(partition by product_name) as diff_avg,
 case
 when current_sales - avg(current_sales) over(partition by product_name) < 0 then 'Below Avg'
 when current_sales - avg(current_sales) over(partition by product_name) > 0 then 'Above Avg'
 else 'Avg'
end  as avg_change,
lag(current_sales) over(partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over(partition by product_name order by order_year) as diff_py,
case
 when current_sales - lag(current_sales) over(partition by product_name order by order_year) > 0 then 'Increase'
 when current_sales - lag(current_sales) over(partition by product_name order by order_year) < 0 then 'Decrease'
 else ' No change'
end as py_change
 from yearly_sales
 order by product_name,order_year;





---Part  to whole Analysis

 with cte_sales as
 (
 select
 p.category as category,
 sum(f.sales_amount) as total_sales
 from gold.dim_products p
 left join gold.fact_sales f
 on p.product_key=f.product_key
 where  f.sales_amount is not null
 group by p.category
 )
 select
 category,
 total_sales,
 sum(total_sales) over() as overall_sales,
 round((cast(total_sales as float)/sum(total_sales) over())*100,2) as perc_of_contribution
 from cte_sales
 order by    total_sales desc






----Product Segmentation 
 with cte_cost as
 (
 select
product_key,
product_name,
cost,
case
when cost<100 then 'below 100'
when cost between 100 and 500 then '100-500'
when cost between 500 and 1000 then '500-1000'
else 'above 1000'
end as cost_range
from gold.dim_products
)
select
cost_range,
count(product_key) as product_count
from cte_cost
group by cost_range
order by count(product_key) DESC






---Grouping customers into three segements based on their spening behaviour
---VIP -at least a history of 12 months and spent more than 5000
--Regular-at least a history of 12 months but spend less than or equal to 5000
---else NEW
with cte_customers as
(
select
c.customer_key ,
min(f.order_date) as first_order,
max(f.order_date) as last_order,
sum(f.sales_amount) as total_spending,
DATEDIFF(month,min(f.order_date),max(f.order_date)) as lifespan
from gold.dim_customers c
left join gold.fact_sales f
on c.customer_key=f.customer_key
group by c.customer_key
), cte_customer_group as
(
select
customer_key,
total_spending,
lifespan,
case
when lifespan>=12 and total_spending >5000 then 'VIP'
when lifespan>=12 and total_spending <=5000 then 'Regular'
else 'New'
end as customer_group
from cte_customers
)
select
customer_group,
count(customer_key) as customers_count
from cte_customer_group
group by customer_group


