---product Report

create view report_products as 
with base_query as (
SELECT
p.product_key,
p.product_name,
p.category,
p.sub_category,
p.cost,
f.order_date,
f.order_number,
f.customer_key,
f.sales_amount,
f.quantity
from gold.dim_products p
left join gold.fact_sales f 
on p.product_key = f.product_key
where order_date is not null
),cte2 as(
select
product_key,
product_name,
category,
sub_category,
cost,
max(order_date) as last_sale_date,
DATEDIFF(month,min(order_date),max(order_date)) as lifespan,
count(distinct order_number) as total_orders,
count(distinct customer_key) as total_customers,
sum(sales_amount) as total_sales,
count(quantity) as total_quantity,
avg(sales_amount/quantity) as avg_selling_price
from base_query                                                    
group by
product_key,
product_name,
category,
sub_category,
cost
)
select
product_key,
product_name,
category,
sub_category,
cost,
last_sale_date,
total_orders,
total_sales,
total_customers,
total_quantity,
datediff(month,last_sale_date,getdate()) as recency_in_months,
case
when total_sales >= 50000 then 'High-range-performer'
when total_sales >=10000 then 'mid-range-performer'
else 'low-range-performer'
end as product_segment,
lifespan,
avg_selling_price,
case
when total_orders =0 then 0
else total_sales/total_orders
end as avg_order_revenue,
case
when lifespan =0 then 0
else total_sales/lifespan 
end as avg_monthly_sales
from cte2

 ---single line query to view the final report of product

select* from report_products


-----Customer Report

create view Report_customers as
with base_query_customer as (
select
c.customer_key,
c.customer_number,
concat(c.first_name, ' ',c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) as age,
f.order_date,
f.order_number,
f.sales_amount,
f.quantity
from gold.dim_customers c
left join gold.fact_sales f
on c.customer_key=f.customer_key
where f.order_date is not null
),customer_agg as(
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
count(quantity) as total_quantity,
datediff(month,min(order_date),max(order_date)) as lifeSpan,
max(order_date) as last_order_date
from base_query_customer
group by
customer_key,
customer_number,
customer_name,
age
)
select
customer_key,
customer_number,
customer_name,
age,
case
when age <20 then 'under 20'
when age between 20 and 29 then '20-29'
when age between 30 and 39 then '30-39'
when age between 40 and 49 then '40-49'
else '50 and above'
end as age_group,
case
when lifespan >=12 and total_sales >5000 then 'VIP'
when lifespan >=12 and total_sales <=5000 then 'Regular'
else 'New'
end as Customer_Segmention,
datediff(month,last_order_date,getdate()) as Recency,
last_order_date,
total_orders,
total_quantity,
total_sales,
LifeSpan,
case
when total_sales =0 then 0
else total_sales/total_orders
end as avg_order_value,
case
when lifespan =0 then 0
else total_sales/lifespan
end as avg_monthly_spend
from customer_agg;


 ----single line query to view the final report of customer 

 select* from Report_customers





 

 


