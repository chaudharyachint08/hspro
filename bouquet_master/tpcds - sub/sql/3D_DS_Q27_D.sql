select *
from store_sales, date_dim, item, customer_demographics
where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_cdemo_sk = cd_demo_sk and cd_gender = 'F' and cd_marital_status = 'D' and cd_education_status = 'Primary' and d_year = 2000 and i_current_price <= 15