select *
from catalog_sales, customer_demographics, date_dim, item
where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cd_gender = 'F' and cd_marital_status = 'U' and cd_education_status = 'Unknown' and d_year = 2002 and i_current_price <= 10