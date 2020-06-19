select *
from catalog_sales, customer_demographics, date_dim, item, promotion
where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cs_promo_sk = p_promo_sk and cd_gender = 'F' and cd_marital_status = 'U' and cd_education_status = 'Unknown' and (p_channel_email = 'N' or p_channel_event = 'N') and d_year = 2002 and i_current_price <= 10