#создаем базу данных для проекта
CREATE DATABASE PROJECT;

# создаем таблицу клиентов
CREATE TABLE customers (
Id_client INT PRIMARY KEY AUTO_INCREMENT,
Total_amount FLOAT,
Gender ENUM('M', 'F'),
Age INT,
Count_city INT,
Response_communcation INT,
Communication_3month INT,
Tenure INT
);

-- загружаем данные через импорт Table Data import Wizard

-- проверяем созданую таблицу
SELECT *
FROM customers;


-- Создаем транзакционную таблицу данных 

CREATE TABLE transactions (
date_new date,
Id_check INT,
ID_client INT,
Count_products DECIMAL, 
Sum_payment DECIMAL
);

-- заполняем данными
-- проверяем таблицу
SELECT *
FROM transactions;


-- 
-- Период (правый край не включаем)
SET @start_dt = DATE('2015-06-01');
SET @end_dt   = DATE('2016-06-01');

WITH
tx AS (  -- транзакции периода + первый день месяца
  SELECT
      ID_client,
      Id_check,
      DATE_FORMAT(date_new, '%Y-%m-01') AS month_start,
      Sum_payment
  FROM transactions
  WHERE date_new >= @start_dt AND date_new < @end_dt
),
check_totals AS (        -- сумма по каждому чеку
  SELECT ID_client, Id_check, SUM(Sum_payment) AS check_amount
  FROM tx
  GROUP BY ID_client, Id_check
),
client_month AS (        -- активность клиента по месяцам
  SELECT ID_client, month_start,
         COUNT(DISTINCT Id_check) AS ops_in_month,
         SUM(Sum_payment)         AS revenue_in_month
  FROM tx
  GROUP BY ID_client, month_start
),
continuous AS (          -- клиенты с 12/12 месяцев
  SELECT ID_client
  FROM client_month
  GROUP BY ID_client
  HAVING COUNT(DISTINCT month_start) = 12
),
client_year AS (         -- итоги клиента за период
  SELECT ID_client,
         SUM(ops_in_month)     AS ops_cnt_period,
         SUM(revenue_in_month) AS revenue_period
  FROM client_month
  GROUP BY ID_client
),
avg_check AS (           -- средний чек клиента (по чекам)
  SELECT ID_client, AVG(check_amount) AS avg_check_period
  FROM check_totals
  GROUP BY ID_client
)
SELECT c.ID_client AS client_id,
       ROUND(a.avg_check_period, 2)    AS avg_check_period,
       ROUND(y.revenue_period / 12, 2) AS avg_month_purchase,
       y.ops_cnt_period                AS ops_cnt_period
FROM continuous c
JOIN client_year y USING (ID_client)
JOIN avg_check a  USING (ID_client)
ORDER BY client_id;

-- Помесячные метрики: средний чек, операции, клиенты, доли от года

-- Настраиваем период (правый край не включаем)
SET @start_dt = DATE('2015-06-01');
SET @end_dt   = DATE('2016-06-01');

WITH
-- Берём только нужный период и добавляем "первое число месяца" для группировки
tx AS (
  SELECT
      ID_client,
      Id_check,
      DATE_FORMAT(date_new, '%Y-%m-01') AS month_start,
      Sum_payment
  FROM transactions
  WHERE date_new >= @start_dt AND date_new < @end_dt
),

-- Схлопываем позиции в чек, чтобы средний чек считался по чекам, а не строкам
check_totals AS (
  SELECT
      ID_client,
      Id_check,
      MIN(month_start) AS month_start,     -- месяц чека
      SUM(Sum_payment) AS check_amount     -- сумма чека
  FROM tx
  GROUP BY ID_client, Id_check
),

-- Итоги по месяцу: сколько чеков, сколько уникальных клиентов, какая выручка
month_totals AS (
  SELECT
      month_start,
      COUNT(DISTINCT Id_check)  AS ops_in_month,
      COUNT(DISTINCT ID_client) AS clients_in_month,
      SUM(Sum_payment)          AS revenue_in_month
  FROM tx
  GROUP BY month_start
),

-- Средний чек в месяце (по чекам)
avg_check_by_month AS (
  SELECT month_start, AVG(check_amount) AS avg_check
  FROM check_totals
  GROUP BY month_start
),

-- Итоги за весь период: общее число операций и общая выручка
year_totals AS (
  SELECT
      COUNT(DISTINCT Id_check) AS year_ops,
      SUM(Sum_payment)         AS year_revenue
  FROM tx
)

-- Финальный срез по месяцам
SELECT
    m.month_start                                                AS month,
    ROUND(a.avg_check, 2)                                        AS avg_check_in_month,      -- средняя сумма чека
    m.ops_in_month                                               AS ops_in_month,            -- сколько операций в месяце
    m.clients_in_month                                           AS clients_in_month,        -- сколько клиентов в месяце
    ROUND(m.ops_in_month   / yt.year_ops      * 100, 2)          AS ops_share_of_year_pct,   -- доля операций от годового итога
    ROUND(m.revenue_in_month / yt.year_revenue * 100, 2)         AS revenue_share_of_year_pct-- доля выручки от годового итога
FROM month_totals m
JOIN avg_check_by_month a USING (month_start)
CROSS JOIN year_totals yt
ORDER BY month;


-- M/F/NA по месяцам: % по операциям и % по затратам (выручке)
SET @start_dt = DATE('2015-06-01');
SET @end_dt   = DATE('2016-06-01');

WITH
-- Транзакции периода + месяц
tx AS (
  SELECT
      t.ID_client,
      t.Id_check,
      DATE_FORMAT(t.date_new, '%Y-%m-01') AS month_start,
      t.Sum_payment
  FROM transactions t
  WHERE t.date_new >= @start_dt AND t.date_new < @end_dt
),

-- Нормализуем пол: пусто/NULL -> 'NA'
gender_map AS (
  SELECT
      c.Id_client,
      COALESCE(NULLIF(TRIM(c.Gender), ''), 'NA') AS Gender
  FROM customers c
),

-- Агрегируем по месяцу и полу: операции и выручка
g_month AS (
  SELECT
      x.month_start,
      gm.Gender,
      COUNT(DISTINCT x.Id_check) AS ops,
      SUM(x.Sum_payment)         AS revenue
  FROM tx x
  LEFT JOIN gender_map gm ON gm.Id_client = x.ID_client
  GROUP BY x.month_start, gm.Gender
)

-- Считаем доли внутри месяца
SELECT
    month_start,
    Gender,
    ops,
    revenue,
    ROUND(ops     / SUM(ops)     OVER (PARTITION BY month_start) * 100, 2) AS ops_share_in_month_pct,     -- % операций M/F/NA в месяце
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY month_start) * 100, 2) AS revenue_share_in_month_pct  -- % выручки M/F/NA в месяце
FROM g_month
ORDER BY month_start, Gender;

-- Итоги за период по возрастным группам (10 лет + NA)
SET @start_dt = DATE('2015-06-01');
SET @end_dt   = DATE('2016-06-01');

WITH
-- Берём транзакции за период
tx AS (
  SELECT
      ID_client,
      Id_check,
      Sum_payment
  FROM transactions
  WHERE date_new >= @start_dt AND date_new < @end_dt
),

-- Карта возрастов: шаг 10 лет, NULL -> NA
age_map AS (
  SELECT
      c.Id_client,
      CASE
        WHEN c.Age IS NULL THEN 'NA'
        ELSE CONCAT(FLOOR(c.Age/10)*10, '-', FLOOR(c.Age/10)*10 + 9)
      END AS age_band
  FROM customers c
),

-- Считаем за период по возрастной группе: кол-во операций, сумма
agg_age AS (
  SELECT
      COALESCE(am.age_band, 'NA') AS age_band,
      COUNT(DISTINCT t.Id_check)  AS ops_cnt_period,
      SUM(t.Sum_payment)          AS sum_period
  FROM tx t
  LEFT JOIN age_map am ON am.Id_client = t.ID_client
  GROUP BY COALESCE(am.age_band, 'NA')
)
SELECT *
FROM agg_age
ORDER BY
  CASE WHEN age_band='NA' THEN 999
       ELSE CAST(SUBSTRING_INDEX(age_band,'-',1) AS UNSIGNED) END;

--  Поквартально: средний чек, операции, выручка, доли в % по возрастным группам
SET @start_dt = DATE('2015-06-01');
SET @end_dt   = DATE('2016-06-01');

WITH
-- Транзакции + месяц
tx AS (
  SELECT
      ID_client,
      Id_check,
      date_new,
      Sum_payment
  FROM transactions
  WHERE date_new >= @start_dt AND date_new < @end_dt
),

-- Чеки схлопнутые по сумме
check_totals AS (
  SELECT
      ID_client,
      Id_check,
      MIN(date_new) AS check_date,
      SUM(Sum_payment) AS check_amount
  FROM tx
  GROUP BY ID_client, Id_check
),

-- Карта возрастов
age_map AS (
  SELECT
      c.Id_client,
      CASE
        WHEN c.Age IS NULL THEN 'NA'
        ELSE CONCAT(FLOOR(c.Age/10)*10, '-', FLOOR(c.Age/10)*10 + 9)
      END AS age_band
  FROM customers c
),

-- Чеки с возрастом и кварталом
checks_age_q AS (
  SELECT
      ct.Id_check,
      COALESCE(am.age_band,'NA') AS age_band,
      CONCAT(YEAR(ct.check_date), '-Q', QUARTER(ct.check_date)) AS yq,
      ct.check_amount
  FROM check_totals ct
  LEFT JOIN age_map am ON am.Id_client = ct.ID_client
),

-- Агрегация по кварталу и возрастной группе
agg_q AS (
  SELECT
      yq,
      age_band,
      COUNT(*)          AS ops,
      SUM(check_amount) AS revenue,
      AVG(check_amount) AS avg_check
  FROM checks_age_q
  GROUP BY yq, age_band
)

-- Финальный вывод с долями в % внутри квартала
SELECT
    yq,
    age_band,
    ops,
    revenue,
    ROUND(avg_check,2) AS avg_check,
    ROUND(ops     / SUM(ops)     OVER (PARTITION BY yq) * 100, 2) AS ops_share_pct_quarter,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY yq) * 100, 2) AS revenue_share_pct_quarter
FROM agg_q
ORDER BY yq,
  CASE WHEN age_band='NA' THEN 999
       ELSE CAST(SUBSTRING_INDEX(age_band,'-',1) AS UNSIGNED) END;

