select *
from call_center, catalog_returns, customer, customer_demographics
where cr_call_center_sk = cc_call_center_sk and cr_returning_customer_sk = c_customer_sk and cd_demo_sk = c_current_cdemo_sk and ((cd_marital_status = 'M' and cd_education_status = 'Unknown') or (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree'))