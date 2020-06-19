select *
from catalog_returns, customer, customer_address, customer_demographics
where cr_returning_customer_sk = c_customer_sk and cd_demo_sk = c_current_cdemo_sk and ca_address_sk = c_current_addr_sk and ((cd_marital_status = 'M' and cd_education_status = 'Unknown') or (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree')) and ca_gmt_offset = -7