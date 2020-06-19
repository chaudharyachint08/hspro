select sm_type, web_name,
sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end) as "30 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end ) as "31-60 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end) as "61-90 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end) as "91-120 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1 else 0 end) as ">120 days"
from web_sales, warehouse, ship_mode, web_site, date_dim
where d_date_sk = ws_ship_date_sk and w_warehouse_sk = ws_warehouse_sk and sm_ship_mode_sk = ws_ship_mode_sk and web_site_sk = ws_web_site_sk and d_year = 2001
group by sm_type, web_name
order by sm_type, web_name