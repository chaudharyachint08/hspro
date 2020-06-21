select *
from store_sales, date_dim, item, promotion
where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_promo_sk = p_promo_sk and d_year = 2001 and ss_list_price <= 1.5