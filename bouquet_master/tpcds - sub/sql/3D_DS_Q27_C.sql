select *
from store_sales, date_dim, item, store
where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_store_sk = s_store_sk and d_year = 2000 and s_state in ('TN') and i_current_price <= 15