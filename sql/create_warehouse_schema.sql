-- Drop tables in reverse order of dependency to avoid foreign key errors
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;

-- Create Dimension Table for Customers
CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    registration_date DATE
);

-- Create Dimension Table for Products
CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(255),
    price DECIMAL(10, 2)
);

-- Create Fact Table for Orders
-- This table connects customers and products.
CREATE TABLE fact_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    order_date DATE,
    -- Define foreign key constraints
    CONSTRAINT fk_customer
        FOREIGN KEY(customer_id) 
        REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_product
        FOREIGN KEY(product_id) 
        REFERENCES dim_product(product_id)
);