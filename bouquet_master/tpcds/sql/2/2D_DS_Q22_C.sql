select i_product_name, i_brand, i_class, i_category, sum(inv_quantity_on_hand)
from inventory, warehouse, item, date_dim
where i_item_sk = inv_item_sk and w_warehouse_sk = inv_warehouse_sk and d_date_sk = inv_date_sk and d_year = 1998
group by i_product_name, i_brand, i_class, i_category