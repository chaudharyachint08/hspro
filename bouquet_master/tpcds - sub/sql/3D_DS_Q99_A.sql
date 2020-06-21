select *
from catalog_sales, warehouse, ship_mode, call_center
where w_warehouse_sk = cs_warehouse_sk and sm_ship_mode_sk = cs_ship_mode_sk and cc_call_center_sk = cs_call_center_sk