-- sample model
CREATE TABLE dw.orders_fact AS
SELECT
  o.id as id,
  o.customer_id as customer_id,
  o.total_amount as amount_usd
FROM orders_db.orders o;
