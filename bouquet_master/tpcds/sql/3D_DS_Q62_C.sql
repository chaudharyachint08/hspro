select *
from web_sales, warehouse, ship_mode, web_site
where w_warehouse_sk = ws_warehouse_sk and sm_ship_mode_sk = ws_ship_mode_sk and web_site_sk = ws_web_site_sk