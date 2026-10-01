INSERT INTO customers
(first_name, last_name, email, phone, city, state)
VALUES
('Ava', 'Patel', 'ava.patel@example.com', '555-1001', 'Austin', 'TX'),
('Noah', 'Kim', 'noah.kim@example.com', '555-1002', 'Seattle', 'WA'),
('Mia', 'Johnson', 'mia.johnson@example.com', '555-1003', 'Chicago', 'IL'),
('Liam', 'Garcia', 'liam.garcia@example.com', '555-1004', 'Phoenix', 'AZ'),
('Emma', 'Brown', 'emma.brown@example.com', '555-1005', 'Boston', 'MA');

INSERT INTO products
(product_name, category, price, inventory_quantity)
VALUES
('Wireless Headphones', 'Electronics', 129.99, 150),
('Mechanical Keyboard', 'Electronics', 89.99, 100),
('Running Shoes', 'Sports', 74.99, 200),
('Coffee Maker', 'Home', 59.99, 80),
('Backpack', 'Accessories', 44.99, 175);

INSERT INTO orders
(customer_id, order_status, order_total, order_timestamp)
VALUES
(1, 'COMPLETED', 219.98, CURRENT_TIMESTAMP - INTERVAL '5 days'),
(2, 'COMPLETED', 74.99, CURRENT_TIMESTAMP - INTERVAL '4 days'),
(3, 'SHIPPED', 149.98, CURRENT_TIMESTAMP - INTERVAL '3 days'),
(4, 'PROCESSING', 59.99, CURRENT_TIMESTAMP - INTERVAL '2 days'),
(5, 'COMPLETED', 134.98, CURRENT_TIMESTAMP - INTERVAL '1 day');

INSERT INTO order_items
(order_id, product_id, quantity, unit_price, line_total)
VALUES
(1, 1, 1, 129.99, 129.99),
(1, 2, 1, 89.99, 89.99),
(2, 3, 1, 74.99, 74.99),
(3, 3, 2, 74.99, 149.98),
(4, 4, 1, 59.99, 59.99),
(5, 2, 1, 89.99, 89.99),
(5, 5, 1, 44.99, 44.99);

INSERT INTO payments
(order_id, payment_method, payment_status, payment_amount, payment_timestamp)
VALUES
(1, 'CREDIT_CARD', 'PAID', 219.98, CURRENT_TIMESTAMP - INTERVAL '5 days'),
(2, 'PAYPAL', 'PAID', 74.99, CURRENT_TIMESTAMP - INTERVAL '4 days'),
(3, 'CREDIT_CARD', 'PAID', 149.98, CURRENT_TIMESTAMP - INTERVAL '3 days'),
(4, 'CREDIT_CARD', 'PENDING', 59.99, CURRENT_TIMESTAMP - INTERVAL '2 days'),
(5, 'APPLE_PAY', 'PAID', 134.98, CURRENT_TIMESTAMP - INTERVAL '1 day');
