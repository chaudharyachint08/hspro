select *
from catalog_sales, warehouse, call_center, date_dim
where d_date_sk = cs_ship_date_sk and w_warehouse_sk = cs_warehouse_sk and cc_call_center_sk = cs_call_center_sk and d_year = 2002