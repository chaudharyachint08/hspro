select i_item_id, i_item_desc, i_current_price
from store_sales, date_dim, item, store
where d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and s_state in ('TN','TN','TN','TN','TN','TN','TN','TN') and d_year = 2001
group by i_item_id, i_item_desc, i_current_price
order by i_item_id