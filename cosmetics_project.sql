--PART I: Data loading. Create a table for each month, from Oct. 2019 to Feb. 2020, and load the data into each table.
CREATE TABLE Oct_to_Feb (
	event_time DateTime,
	event_type varchar(30),
	product_id varchar(50),
	category_id varchar(50),
	category_code varchar(50),
	brand varchar(20),
	price float,
	user_id varchar(20),
	user_session varchar(50));

BULK INSERT Oct_to_Feb
FROM 'C:\SQL_Project\2019-Oct.csv'
WITH (FORMAT = 'CSV'
	, FIRSTROW=2
	, FIELDTERMINATOR = ','
	, ROWTERMINATOR = '0x0a');

--Load the data of the other 4 months. 
BULK INSERT Oct_to_Feb
FROM 'C:\SQL_Project\2019-Nov.csv'
WITH (FORMAT = 'CSV'
	, FIRSTROW=2
	, FIELDTERMINATOR = ','
	, ROWTERMINATOR = '0x0a');

BULK INSERT Oct_to_Feb
FROM 'C:\SQL_Project\2019-Dec.csv'
WITH (FORMAT = 'CSV'
	, FIRSTROW=2
	, FIELDTERMINATOR = ','
	, ROWTERMINATOR = '0x0a');

BULK INSERT Oct_to_Feb
FROM 'C:\SQL_Project\2020-Jan.csv'
WITH (FORMAT = 'CSV'
	, FIRSTROW=2
	, FIELDTERMINATOR = ','
	, ROWTERMINATOR = '0x0a');

BULK INSERT Oct_to_Feb
FROM 'C:\SQL_Project\2020-Feb.csv'
WITH (FORMAT = 'CSV'
	, FIRSTROW=2
	, FIELDTERMINATOR = ','
	, ROWTERMINATOR = '0x0a');

SELECT * FROM Oct_to_Feb;

--Part II: Data inspection and cleaning.
  -- Checking for NULL values.
SELECT COUNT(*) FROM Oct_to_Feb; -- In total, there are 20,692,840 rows
SELECT COUNT(*) FROM Oct_to_Feb WHERE event_time IS NULL; --There is no NULL value for event_type
SELECT COUNT(*) FROM Oct_to_Feb WHERE event_type IS NULL; --No NULL.
SELECT COUNT(*) FROM Oct_to_Feb WHERE product_id IS NULL; --No NULL.
SELECT COUNT(*) FROM Oct_to_Feb WHERE category_id IS NULL; --No NULL.
SELECT COUNT(*) FROM Oct_to_Feb WHERE category_code IS NULL; --20,339,246 NULLs.
SELECT COUNT(*) FROM Oct_to_Feb WHERE brand IS NULL; --8,757,117 NULLs.
SELECT COUNT(*) FROM Oct_to_Feb WHERE price IS NULL; --No NULL.
SELECT COUNT(*) FROM Oct_to_Feb WHERE user_id IS NULL; --No NULL.
SELECT COUNT(*) FROM Oct_to_Feb WHERE user_session IS NULL; --4,598.

-- Checking for negative prices
SELECT *
FROM Oct_to_Feb
WHERE price < 0; -- found 131 records
  -- Drop these rows
DELETE FROM Oct_to_Feb
WHERE price < 0;
  -- Check if it has taken effect.
SELECT COUNT(*)
FROM Oct_to_Feb
WHERE price < 0; -- Yes it has.

--Part III: data exploration and KPIs.

CREATE VIEW Oct_2019 AS
	SELECT *
	FROM Oct_to_Feb
	WHERE DATEPART(MONTH, event_time) = 10;

SELECT COUNT(DISTINCT product_id) AS distinct_product_id, -- 41,894
       COUNT(DISTINCT category_id) AS distinct_category_id, --490
       COUNT(DISTINCT user_id) AS distinct_user_id, --399,664
       COUNT(DISTINCT user_session) AS total_user_sessions --873,960
FROM Oct_2019;

  --KPIs I: session KPIs.
SELECT COUNT(DISTINCT user_session) AS sessions_with_product_views_oct
FROM Oct_2019
WHERE event_type = 'view'; --826,147

SELECT COUNT(DISTINCT user_session) AS sessions_with_add_to_cart
FROM Oct_2019
WHERE event_type = 'cart') ; --228,145

SELECT COUNT(DISTINCT user_session) AS sessions_with_remove_from_cart
FROM Oct_2019
WHERE event_type = 'remove_from_cart'; --91,973

SELECT COUNT(DISTINCT user_session) AS sessions_with_check_out
FROM Oct_2019
WHERE event_type = 'purchase'; --29,326
--The above calculations can be done to the other 4 months.
--We could easily calculate add_to_cart rate for Oct.: 228145/826147*100 --27.6%
--And cart_abandonment_rate: (1-29326/228145)*100 --87.1%

--KPIs II: per-session value; conversion rate;
  --Assumptions:one user_session have multiple records of 'purchase', assuming each record equals one product
SELECT ROUND(SUM(price),2) AS total_revenue,
	   ROUND(SUM(price)/873960*100,2) AS per_session_value
FROM Oct_2019
WHERE event_type = 'purchase';--per_session_value:138.63

SELECT 
	(SELECT COUNT(DISTINCT user_session) 
	FROM Oct_2019
	WHERE event_type = 'purchase') AS number_of_transactions, --number of transactions:29326
	COUNT(DISTINCT user_session) AS total_user_sessions,--873960
	ROUND((SELECT COUNT(DISTINCT user_session) FROM Oct_2019 WHERE event_type = 'purchase')/COUNT(DISTINCT user_session)*100,2) AS conversion_rate
FROM Oct_2019; --conversation rate:3.36%

--KPIs III: repeat_customers and one_time_customers.
CREATE VIEW all_customers AS
	SELECT user_id, user_session, SUM(price) AS order_value,
		ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY user_id) row_num
	FROM Oct_to_Feb
	WHERE event_type = 'purchase'
	GROUP BY user_id, user_session;

SELECT * FROM all_customers;

   --Repeat customers.
SELECT COUNT(user_id) AS total_repeat_customers, --23,303
	   ROUND(SUM(customer_spent),2) AS repeat_customer_revenue  --repeat customer revenue: 3,051,692.59
FROM
	(SELECT user_id, SUM(order_value) AS customer_spent, SUM(row_num) AS X
	FROM all_customers
	GROUP BY user_id
	HAVING SUM(row_num) > 1) repeat_customer; -- We can calculate that on average a repeat customer spent: 131.0.


	--One-time customers.
SELECT COUNT(user_id) AS total_one_time_customers, --87,215
	   ROUND(SUM(customer_spent),2) AS one_time_customer_revenue  --3,300,137.7
FROM
	(SELECT user_id, SUM(order_value) AS customer_spent, SUM(row_num) AS X
	FROM all_customers
	GROUP BY user_id
	HAVING SUM(row_num) = 1) one_time_customer;-- We can calculate that on average an one-time customer spent: 37.8.

SELECT COUNT(DISTINCT user_id) AS total_customers
FROM Oct_to_Feb
WHERE event_type = 'purchase'; --11O,518.
--We can easily calculate the repeat_purchase_rate = 21.1%

--Trend over time.
  -- Extract day and hour information from the event_time column for Oct.
SELECT 
	COUNT(event_type) AS num_of_events,
	DATEPART(DAY, event_time) AS event_day,
	DATEPART(WEEKDAY, event_time) AS event_weekday
FROM Oct_2019
GROUP BY DATEPART(DAY, event_time),DATEPART(WEEKDAY, event_time)
ORDER BY COUNT(event_type) DESC;
--The beginning of October(2,6,7,8,1,9) has higher number of visits. Weekday-wise, Sundays have the lowest number of events.

  --From Oct. to Feb.
SELECT 
	COUNT(event_type) AS num_of_events,
	DATEPART(MONTH, event_time) AS event_month,
	DATEPART(DAY, event_time) AS event_day,
	DATEPART(WEEKDAY, event_time) AS event_weekday
FROM Oct_to_Feb
GROUP BY DATEPART(MONTH, event_time), DATEPART(DAY, event_time),DATEPART(WEEKDAY, event_time)
ORDER BY COUNT(event_type) DESC;
--Black Friday sales. Times around Black Friday, 11-22,11-28,11-21,11-24,11-29,11-23 has the highest number of shopping events in a day.