-- Create an SQL statement schema with three related tables: Customers, Orders, Products. Add relationships: Orders reference Customers, and an order can have multiple products via an OrderProducts join table.

CREATE TABLE Customers (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(20)
);

CREATE TABLE Orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    total_price DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES Customers(id)
);

CREATE TABLE Products (
    id INTEGER PRIMARY KEY,
    itemname VARCHAR(255) NOT NULL,
    item_colour VARCHAR(50)
);

CREATE TABLE OrderProducts (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER DEFAULT 1,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES Orders(id),
    FOREIGN KEY (product_id) REFERENCES Products(id)
);

-- Create a reusable calendar table
-- Reusable Calendar Table (self-populating)
CREATE TABLE Calendar (
    calendar_date DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    weekday INTEGER NOT NULL, -- 1=Monday, 7=Sunday
    week INTEGER NOT NULL
);

-- Its self populated
-- Populate Calendar table for 2024 using a recursive CTE (example for SQLite/PostgreSQL)
INSERT INTO Calendar (calendar_date, year, month, day, weekday, week)
WITH RECURSIVE dates AS (
    SELECT DATE('2024-01-01') AS calendar_date
    UNION ALL
    SELECT DATE(calendar_date, '+1 day')
    FROM dates
    WHERE calendar_date < '2024-12-31'
)
SELECT
    calendar_date,
    CAST(STRFTIME('%Y', calendar_date) AS INTEGER) AS year,
    CAST(STRFTIME('%m', calendar_date) AS INTEGER) AS month,
    CAST(STRFTIME('%d', calendar_date) AS INTEGER) AS day,
    CAST(STRFTIME('%w', calendar_date) AS INTEGER) + 1 AS weekday, -- 1=Monday, 7=Sunday
    CAST(STRFTIME('%W', calendar_date) AS INTEGER) + 1 AS week
FROM dates;

-- Based on schema.sql create multiple insert statements with values for each table, i need around 20 values for each
-- Insert Customers
INSERT INTO Customers (id, email, phone_number) VALUES (1, 'alice@example.com', '555-0101');
INSERT INTO Customers (id, email, phone_number) VALUES (2, 'bob@example.com', '555-0102');
INSERT INTO Customers (id, email, phone_number) VALUES (3, 'carol@example.com', '555-0103');
INSERT INTO Customers (id, email, phone_number) VALUES (4, 'dave@example.com', '555-0104');
INSERT INTO Customers (id, email, phone_number) VALUES (5, 'eve@example.com', '555-0105');
INSERT INTO Customers (id, email, phone_number) VALUES (6, 'frank@example.com', '555-0106');
INSERT INTO Customers (id, email, phone_number) VALUES (7, 'grace@example.com', '555-0107');
INSERT INTO Customers (id, email, phone_number) VALUES (8, 'heidi@example.com', '555-0108');
INSERT INTO Customers (id, email, phone_number) VALUES (9, 'ivan@example.com', '555-0109');
INSERT INTO Customers (id, email, phone_number) VALUES (10, 'judy@example.com', '555-0110');
INSERT INTO Customers (id, email, phone_number) VALUES (11, 'mallory@example.com', '555-0111');
INSERT INTO Customers (id, email, phone_number) VALUES (12, 'oscar@example.com', '555-0112');
INSERT INTO Customers (id, email, phone_number) VALUES (13, 'peggy@example.com', '555-0113');
INSERT INTO Customers (id, email, phone_number) VALUES (14, 'trent@example.com', '555-0114');
INSERT INTO Customers (id, email, phone_number) VALUES (15, 'victor@example.com', '555-0115');
INSERT INTO Customers (id, email, phone_number) VALUES (16, 'wendy@example.com', '555-0116');
INSERT INTO Customers (id, email, phone_number) VALUES (17, 'xavier@example.com', '555-0117');
INSERT INTO Customers (id, email, phone_number) VALUES (18, 'yvonne@example.com', '555-0118');
INSERT INTO Customers (id, email, phone_number) VALUES (19, 'zach@example.com', '555-0119');
INSERT INTO Customers (id, email, phone_number) VALUES (20, 'quinn@example.com', '555-0120');

-- Insert Products
INSERT INTO Products (id, itemname, item_colour) VALUES (1, 'Laptop', 'Silver');
INSERT INTO Products (id, itemname, item_colour) VALUES (2, 'Phone', 'Black');
INSERT INTO Products (id, itemname, item_colour) VALUES (3, 'Headphones', 'White');
INSERT INTO Products (id, itemname, item_colour) VALUES (4, 'Monitor', 'Black');
INSERT INTO Products (id, itemname, item_colour) VALUES (5, 'Keyboard', 'Gray');
INSERT INTO Products (id, itemname, item_colour) VALUES (6, 'Mouse', 'Blue');
INSERT INTO Products (id, itemname, item_colour) VALUES (7, 'Printer', 'White');
INSERT INTO Products (id, itemname, item_colour) VALUES (8, 'Speaker', 'Red');
INSERT INTO Products (id, itemname, item_colour) VALUES (9, 'Camera', 'Black');
INSERT INTO Products (id, itemname, item_colour) VALUES (10, 'Headphones', 'Green');
INSERT INTO Products (id, itemname, item_colour) VALUES (11, 'Charger', 'White');
INSERT INTO Products (id, itemname, item_colour) VALUES (12, 'Router', 'Black');
INSERT INTO Products (id, itemname, item_colour) VALUES (13, 'SSD', 'Silver');
INSERT INTO Products (id, itemname, item_colour) VALUES (14, 'USB Drive', 'Blue');
INSERT INTO Products (id, itemname, item_colour) VALUES (15, 'Webcam', 'Black');
INSERT INTO Products (id, itemname, item_colour) VALUES (16, 'Microphone', 'Gray');
INSERT INTO Products (id, itemname, item_colour) VALUES (17, 'Smartwatch', 'Gold');
INSERT INTO Products (id, itemname, item_colour) VALUES (18, 'Drone', 'White');
INSERT INTO Products (id, itemname, item_colour) VALUES (19, 'Headphones', 'Black');
INSERT INTO Products (id, itemname, item_colour) VALUES (20, 'VR Headset', 'Black');

-- Insert Orders
INSERT INTO Orders (id, customer_id, total_price) VALUES (1, 1, 1200.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (2, 2, 800.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (3, 3, 450.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (4, 4, 300.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (5, 5, 150.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (6, 6, 220.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (7, 7, 600.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (8, 8, 900.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (9, 9, 1100.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (10, 10, 350.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (11, 11, 400.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (12, 12, 500.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (13, 13, 700.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (14, 14, 250.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (15, 15, 100.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (16, 16, 950.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (17, 17, 800.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (18, 18, 650.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (19, 19, 120.00);
INSERT INTO Orders (id, customer_id, total_price) VALUES (20, 20, 2000.00);

-- Insert OrderProducts
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (1, 1, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (1, 5, 2);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (2, 2, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (2, 6, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (3, 3, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (3, 7, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (4, 4, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (4, 8, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (5, 5, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (5, 9, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (6, 6, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (6, 10, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (7, 7, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (7, 11, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (8, 8, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (8, 12, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (9, 9, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (9, 13, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (10, 10, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (10, 14, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (11, 11, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (11, 15, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (12, 12, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (12, 16, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (13, 13, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (13, 17, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (14, 14, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (14, 18, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (15, 15, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (15, 19, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (16, 16, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (16, 20, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (17, 17, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (17, 1, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (18, 18, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (18, 2, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (19, 19, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (19, 3, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (20, 20, 1);
INSERT INTO OrderProducts (order_id, product_id, quantity) VALUES (20, 4, 1);

-- Generate join quiries where you aggregate by colour and filter by the most sold colour
-- Aggregate total quantity sold by product colour
SELECT
    p.item_colour,
    SUM(op.quantity) AS total_sold
FROM
    OrderProducts op
    JOIN Products p ON op.product_id = p.id
GROUP BY
    p.item_colour;

-- Find the most sold colour
SELECT
    p.item_colour,
    SUM(op.quantity) AS total_sold
FROM
    OrderProducts op
    JOIN Products p ON op.product_id = p.id
GROUP BY
    p.item_colour
ORDER BY
    total_sold DESC
LIMIT 1;

-- Create a sub query for aggregation
-- sub query version
-- Aggregate total quantity sold by product colour using a subquery
SELECT *
FROM (
    SELECT
        p.item_colour,
        SUM(op.quantity) AS total_sold
    FROM
        OrderProducts op
        JOIN Products p ON op.product_id = p.id
    GROUP BY
        p.item_colour
) AS colour_sales
ORDER BY
    total_sold DESC;

-- Optimise the sub query version by removing the sub query
-- optimised version to find the most sold colour
-- Aggregate total quantity sold by product colour (optimized, no subquery)
SELECT
    p.item_colour,
    SUM(op.quantity) AS total_sold
FROM
    OrderProducts op
    JOIN Products p ON op.product_id = p.id
GROUP BY
    p.item_colour
ORDER BY
    total_sold DESC;

-- Calculate number of people that ordered a Headphones
-- Calculate number of distinct customers that ordered Headphones
SELECT COUNT(DISTINCT o.customer_id) AS num_customers_headphones
FROM Orders o
JOIN OrderProducts op ON o.id = op.order_id
JOIN Products p ON op.product_id = p.id
WHERE p.itemname = 'Headphones';
