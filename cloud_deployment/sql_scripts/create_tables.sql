CREATE TABLE IF NOT EXISTS raw_layer.sales (
    invoice_id STRING,
    branch STRING,
    city STRING,
    customer_type STRING,
    gender STRING,
    product_line STRING,
    unit_price FLOAT64,
    quantity INT64,
    tax FLOAT64,
    total FLOAT64,
    t_date DATE,
    t_time TIME,
    payment STRING,
    cogs FLOAT64,
    gross_margin_percentage FLOAT64,
    gross_income FLOAT64,
    rating FLOAT64
);


CREATE TABLE IF NOT EXISTS confirmed_layer.product (
    product_id STRING,
    product_line STRING,
    unit_price FLOAT64,
    gross_margin_percentage FLOAT64
);


CREATE TABLE IF NOT EXISTS confirmed_layer.branch (
    branch_id STRING,
    branch STRING,
    city STRING
);

CREATE TABLE IF NOT EXISTS confirmed_layer.sales (
    invoice_id STRING,
    branch_id STRING ,
    customer_type STRING,
    gender STRING,
    product_id STRING,
    quantity INT64,
    tax FLOAT64,
    total FLOAT64,
    t_date DATE,
    t_time TIME,
    payment STRING,
    cogs FLOAT64,
    gross_income FLOAT64,
    rating FLOAT64,
);
