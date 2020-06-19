select *
from web_sales, ship_mode, web_site, date_dim
where d_date_sk = ws_ship_date_sk and sm_ship_mode_sk = ws_ship_mode_sk and web_site_sk = ws_web_site_sk and d_year = 2001