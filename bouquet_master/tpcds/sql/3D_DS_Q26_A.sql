select *
from catalog_sales, customer_demographics, item, promotion
where cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cs_promo_sk = p_promo_sk and cd_gender = 'F' and cd_marital_status = 'U' and cd_education_status = 'Unknown' and (p_channel_email = 'N' or p_channel_event = 'N') and i_current_price <= 10