CREATE TABLE IF NOT EXISTS product (
    product_id TEXT PRIMARY KEY,
    product_line TEXT,
    unit_price REAL,
    gross_margin_percentage REAL
);


CREATE TABLE IF NOT EXISTS branch (
    branch_id TEXT PRIMARY KEY,
    branch TEXT,
    city TEXT
);

CREATE TABLE IF NOT EXISTS  sales (
    invoice_id TEXT PRIMARY KEY,
    branch_id TEXT ,
    customer_type TEXT,
    gender TEXT,
    product_id TEXT,
    quantity INTEGER,
    tax REAL,
    total REAL,
    t_date TEXT,
    t_time TEXT,
    payment TEXT,
    cogs REAL,
    gross_income REAL,
    rating REAL,
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (branch_id) REFERENCES branch(branch_id)
);