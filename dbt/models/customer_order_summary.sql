SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    c.state,

    COUNT(DISTINCT o.order_id) AS total_orders,

    COALESCE(SUM(o.order_total), 0) AS total_order_value,

    COALESCE(SUM(
        CASE
            WHEN p.payment_status = 'PAID'
            THEN p.payment_amount
            ELSE 0
        END
    ), 0) AS total_paid_amount

FROM local.ecommerce.customers_current c

LEFT JOIN local.ecommerce.orders_current o
    ON c.customer_id = o.customer_id

LEFT JOIN local.ecommerce.payments_current p
    ON o.order_id = p.order_id

GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    c.state
