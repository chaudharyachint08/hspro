select w_state, i_item_id, sum(cs_sales_price)
from catalog_sales, warehouse, item, date_dim
where i_item_sk = cs_item_sk and w_warehouse_sk = cs_warehouse_sk and d_date_sk = cs_sold_date_sk and i_current_price between 0.99 and 1.49 and d_date between (cast ('2001-01-17' as date)) and (cast ('2001-03-17' as date))
group by w_state,i_item_id
order by w_state,i_item_id