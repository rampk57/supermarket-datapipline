-- Load confirmed layer tables
CREATE OR REPLACE PROCEDURE confirmed_layer.load_dimensions_and_fact()
BEGIN
    -- Populate the `product` dimension table
    INSERT INTO confirmed_layer.product (product_id, product_line, unit_price, gross_margin_percentage)
    SELECT 
        GENERATE_UUID() AS product_id, 
        product_line, 
        unit_price, 
        gross_margin_percentage
    FROM (
        SELECT DISTINCT 
            product_line, 
            unit_price, 
            gross_margin_percentage
        FROM raw_layer.sales
    ) AS unique_products;

    -- Populate the `branch` dimension table
    INSERT INTO confirmed_layer.branch (branch_id, branch, city)
    SELECT 
        GENERATE_UUID() AS branch_id, 
        branch, 
        city
    FROM (
        SELECT DISTINCT 
            branch, 
            city
        FROM raw_layer.sales
    ) AS unique_branches;

    -- Populate the `sales` fact table
    INSERT INTO confirmed_layer.sales (
        invoice_id, branch_id, customer_type, gender, product_id, quantity, tax, 
        total, t_date, t_time, payment, cogs, gross_income, rating
    )
    SELECT 
        r.invoice_id,
        b.branch_id,
        r.customer_type,
        r.gender,
        p.product_id,
        r.quantity,
        r.tax,
        r.total,
        r.t_date,
        r.t_time,
        r.payment,
        r.cogs,
        r.gross_income,
        r.rating
    FROM 
        raw_layer.sales r
    INNER JOIN 
        confirmed_layer.product p
    ON 
        r.product_line = p.product_line AND 
        r.unit_price = p.unit_price AND 
        r.gross_margin_percentage = p.gross_margin_percentage
    INNER JOIN 
        confirmed_layer.branch b
    ON 
        r.branch = b.branch AND 
        r.city = b.city;
END;

-- Top 3 selling categories on each store based on sale amount (If more than one product takes any place, include them as well)
CREATE OR REPLACE PROCEDURE analytics_layer.top_3_products_by_store()
BEGIN

CREATE OR REPLACE TABLE analytics_layer.top_3_products_by_store AS
WITH product_sales AS 
(SELECT p.product_line AS product_line,b.branch AS branch, SUM(s.total) total_sale,
        RANK() OVER (PARTITION BY branch ORDER BY SUM(s.total) DESC) rnk 
    FROM confirmed_layer.sales s JOIN confirmed_layer.product p USING(product_id) 
    JOIN confirmed_layer.branch b USING(branch_id)
GROUP BY 1,2)

SELECT product_line,branch,total_sale FROM product_sales
WHERE rnk <= 3 
ORDER BY 2,3 DESC;

END;

-- For each product line, which store sold the most (If more than one store takes the place, include them as well)
CREATE OR REPLACE PROCEDURE analytics_layer.top_store_by_product()
BEGIN

CREATE OR REPLACE TABLE analytics_layer.top_store_by_product AS
WITH product_sales AS 
(SELECT p.product_line AS product_line,b.branch AS branch, SUM(s.total) total_sale,
        RANK() OVER (PARTITION BY product_line ORDER BY SUM(s.total) DESC) rnk 
    FROM confirmed_layer.sales s JOIN confirmed_layer.product p  USING(product_id) 
    JOIN confirmed_layer.branch b USING(branch_id)
GROUP BY 1,2)

SELECT product_line,branch,total_sale FROM product_sales
WHERE rnk =1  
ORDER BY 1 DESC;
END;


-- Which product sold as last product in each store many times
CREATE OR REPLACE PROCEDURE analytics_layer.max_last_sold()
BEGIN

CREATE OR REPLACE TABLE analytics_layer.max_last_sold AS
WITH list_last_sales AS 
(SELECT b.branch AS branch, p.product_line AS product_line, s.t_date AS sale_date, s.t_time AS sale_time,
        ROW_NUMBER() OVER (PARTITION BY b.branch,s.t_date  ORDER BY s.t_time DESC) AS row_num
    FROM confirmed_layer.sales s JOIN confirmed_layer.product p USING(product_id)
    JOIN confirmed_layer.branch b USING(branch_id)),
last_sales AS (
    SELECT branch, product_line, count(*) no_days,ROW_NUMBER() OVER (PARTITION BY branch ORDER BY count(*) DESC) AS row_num FROM list_last_sales WHERE row_num = 1
    GROUP BY 1,2
)

SELECT branch,product_line,no_days FROM last_sales
WHERE row_num =1;
END;

