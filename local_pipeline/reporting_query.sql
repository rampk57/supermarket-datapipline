
-- Assumption is that, below reporting queries will be excetued individually
-- Top 3 selling categories on each store based on sale amount (If more than one product takes any place, include them as well)
CREATE TABLE top_3_products_by_store AS
WITH product_sales AS 
(SELECT p.product_line AS product_line,b.branch AS branch, SUM(s.total) total_sale,
        RANK() OVER (PARTITION BY branch ORDER BY SUM(s.total) DESC) rnk 
    FROM sales s JOIN product p USING(product_id) 
    JOIN branch b USING(branch_id)
GROUP BY 1,2)

SELECT product_line,branch,total_sale FROM product_sales
WHERE rnk <= 3 
ORDER BY 2,3 DESC;


-- For each product line, which store sold the most (If more than one store takes the place, include them as well)
CREATE TABLE top_store_by_product AS
WITH product_sales AS 
(SELECT p.product_line AS product_line,b.branch AS branch, SUM(s.total) total_sale,
        RANK() OVER (PARTITION BY product_line ORDER BY SUM(s.total) DESC) rnk 
    FROM sales s JOIN product p  USING(product_id) 
    JOIN branch b USING(branch_id)
GROUP BY 1,2)

SELECT product_line,branch,total_sale FROM product_sales
WHERE rnk =1  
ORDER BY 1 DESC;


-- Which product sold as last product in each store many times
CREATE TABLE max_last_sold AS
WITH list_last_sales AS 
(SELECT b.branch AS branch, p.product_line AS product_line, s.t_date AS sale_date, s.t_time AS sale_time,
        ROW_NUMBER() OVER (PARTITION BY b.branch,s.t_date  ORDER BY s.t_time DESC) AS row_num
    FROM sales s JOIN product p USING(product_id)
    JOIN branch b USING(branch_id)),
last_sales AS (
    SELECT branch, product_line, count(*) no_days,ROW_NUMBER() OVER (PARTITION BY branch ORDER BY count(*) DESC) AS row_num FROM list_last_sales WHERE row_num = 1
    GROUP BY 1,2
)

SELECT branch,product_line,no_days FROM last_sales
WHERE row_num =1;