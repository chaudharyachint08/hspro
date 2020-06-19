select *
from catalog_returns, customer, customer_demographics, household_demographics
where cr_returning_customer_sk = c_customer_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and ((cd_marital_status = 'M' and cd_education_status = 'Unknown') or (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree')) and hd_buy_potential like '5001-10000%'