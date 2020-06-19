select *
from item, store_sales, date_dim, store
where i_item_sk = ss_item_sk and d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and d_year in (1999) and i_category in ('Jewelry', 'Electronics', 'Music') and i_class in ('mens watch', 'wireless', 'classical')