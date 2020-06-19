select *
from catalog_sales, warehouse, ship_mode, date_dim
where d_date_sk = cs_ship_date_sk and w_warehouse_sk = cs_warehouse_sk and sm_ship_mode_sk = cs_ship_mode_sk and d_year = 2002