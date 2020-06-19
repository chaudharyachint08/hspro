select *
from web_sales, warehouse, web_site, date_dim
where d_date_sk = ws_ship_date_sk and w_warehouse_sk = ws_warehouse_sk and web_site_sk = ws_web_site_sk and d_year = 2001