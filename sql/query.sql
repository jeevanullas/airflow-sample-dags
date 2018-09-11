select distinct product_name from product where product_id IN 
  (select product_id from orderline where order_id 
    IN (select order_id from order_info where customer_id=%(cust_id)s));
