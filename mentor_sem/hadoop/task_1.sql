-- delete from transactions_v2 tv
-- where amount < 0 or currency like '%?%';

-- select * from transactions_v2 tv limit 1
-- 1. ■	Фильтрация «хороших» валют (USD, EUR, RUB), подсчёт суммарной суммы транзакций по каждой валюте.
select
	tv.currency,
	SUM(tv.amount)
from transactions_v2 tv

group by tv.currency
having tv.currency in ('USD', 'EUR', 'RUB')

-- 2. ■	Подсчёт количества мошеннических (is_fraud=1) и нормальных (is_fraud=0) транзакций, суммарной суммы и среднего чека.
select
	case tv.is_fraud when 1 then 'is_fraud'
	else 'is_not_fraud'
	end,
	count(*)

from transactions_v2 tv
group by tv.is_fraud

-- 3.■	Группировка по датам с вычислением ежедневного количества транзакций, суммарного объёма и среднего amount.

select
	cast(tv.transaction_date as date) as transaction_date,
	count(*) as number_of_trans,
	sum(tv.amount) as sum_trans,
	avg(tv.amount) as average

from transactions_v2 tv
group by CAST(tv.transaction_date AS DATE)

-- 4.■	JOIN с таблицей logs_v2 по transaction_id, чтобы посчитать количество логов на одну транзакцию, выделить самые частые категории category и т.д.

select
	tv.transaction_id,
	count(lv.log_id)
from transactions_v2 tv

join logs_v2 lv
on tv.transaction_id = lv.transaction_id

group by tv.transaction_id
