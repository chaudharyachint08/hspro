select *
from store_sales, item, store, customer_demographics
where ss_item_sk = i_item_sk and ss_store_sk = s_store_sk and ss_cdemo_sk = cd_demo_sk and cd_gender = 'F' and cd_marital_status = 'D' and cd_education_status = 'Primary' and s_state in ('TN') and i_current_price <= 15