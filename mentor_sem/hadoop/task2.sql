-- Вставка данных

CREATE TABLE orders (
    order_id UInt32,
    user_id UInt32,
    order_date DateTime,
    total_amount Float64,
    payment_status String
)
ENGINE = MergeTree
ORDER BY (order_date, order_id);


INSERT INTO orders VALUES
(1001,10,'2023-03-01 10:00:00',1200.0,'paid'),
(1002,11,'2023-03-01 10:05:00',999.5,'pending'),
(1003,10,'2023-03-01 10:10:00',0.0,'cancelled'),
(1004,12,'2023-03-01 11:00:00',1450.0,'paid'),
(1005,10,'2023-03-01 12:00:00',500.0,'paid'),
(1006,13,'2023-03-02 09:00:00',2100.0,'paid'),
(1007,14,'2023-03-02 09:30:00',300.0,'pending'),
(1008,15,'2023-03-02 10:00:00',450.0,'paid'),
(1009,10,'2023-03-02 10:15:00',1000.0,'pending'),
(1010,11,'2023-03-02 11:00:00',799.0,'paid'),
(1011,12,'2023-03-02 12:00:00',120.0,'cancelled'),
(1012,13,'2023-03-03 08:00:00',2000.0,'paid'),
(1013,15,'2023-03-03 09:00:00',450.0,'paid'),
(1014,15,'2023-03-03 09:30:00',899.99,'paid'),
(1015,14,'2023-03-03 10:00:00',1350.0,'paid'),
(1016,10,'2023-03-03 11:00:00',750.0,'pending');



CREATE TABLE order_items (
    item_id UInt32,
    order_id UInt32,
    product_name String,
    product_price Float64,
    quantity UInt32
)
ENGINE = MergeTree
ORDER BY (order_id, item_id);


INSERT INTO order_items VALUES
(1,1001,'Smartphone',600.0,2),
(2,1002,'Laptop',999.5,1),
(3,1004,'Monitor',300.0,2),
(4,1004,'Keyboard',50.0,1),
(5,1007,'Mouse',25.0,2),
(6,1010,'Laptop',799.0,1),
(7,1019,'Laptop',1100.0,2),
(8,1020,'Speaker',185.5,3),
(9,1009,'Tablet',500.0,2),
(10,1011,'PhoneCase',20.0,3),
(11,1012,'GamingConsole',650.0,3),
(12,1013,'Book',15.0,10),
(13,1014,'Smartwatch',300.0,1),
(14,1015,'Monitor',300.0,2),
(15,1015,'Keyboard',50.0,1),
(16,1016,'Camera',250.0,2);


-- Группировка по payment_status: подсчитываем количество заказов, сумму (total_amount), среднюю стоимость заказа.

select
    payment_status,
    count(*),
    sum(total_amount),
    avg(total_amount)

from orders o

group by payment_status

-- JOIN с order_items: подсчитать общее количество товаров, общую сумму, среднюю цену за продукт

select
    order_id,
    count(*) as products_in_order,
    sum(product_price) as total_amount,
    avg(product_price) as avg_amount


from orders o

join order_items oi
using(order_id)

group by order_id

-- Отдельно посмотреть статистику по датам (количество заказов и их суммарная стоимость за каждый день).

select
    cast(order_date as date) as date,
    count(*) as orders,
    sum(total_amount) as day_amount


from orders o

group by cast(order_date as date)

-- Выделить «самых активных» пользователей (по сумме заказов или по количеству заказов).

    select
        user_id,
        count(*) as orders,
        sum(total_amount) amount
    from orders
    group by user_id
    order by amount desc
    limit 1

    union all

    select
        user_id,
        count(*) as orders,
        sum(total_amount) amount
    from orders
    group by user_id
    order by orders desc
    limit 1