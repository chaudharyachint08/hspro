select *
from catalog_sales, date_dim, item, promotion
where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_promo_sk = p_promo_sk and (p_channel_email = 'N' or p_channel_event = 'N') and d_year = 2002 and i_current_price <= 10