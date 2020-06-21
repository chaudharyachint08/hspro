select *
from catalog_sales, ship_mode, call_center, date_dim
where d_date_sk = cs_ship_date_sk and sm_ship_mode_sk = cs_ship_mode_sk and cc_call_center_sk = cs_call_center_sk and d_year = 2002