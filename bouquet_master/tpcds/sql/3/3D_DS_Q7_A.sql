select *
from store_sales, customer_demographics, item, promotion
where ss_item_sk = i_item_sk and ss_cdemo_sk = cd_demo_sk and ss_promo_sk = p_promo_sk and cd_gender = 'F' and cd_marital_status = 'M' and cd_education_status = 'College' and ss_list_price <= 1.5