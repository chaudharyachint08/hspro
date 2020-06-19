select *
from catalog_returns, date_dim, customer, customer_demographics
where cr_returned_date_sk = d_date_sk and cr_returning_customer_sk = c_customer_sk and cd_demo_sk = c_current_cdemo_sk and d_year = 2000 and d_moy = 12 and ((cd_marital_status = 'M' and cd_education_status = 'Unknown') or (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree'))