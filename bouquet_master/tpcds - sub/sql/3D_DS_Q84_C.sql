select *
from customer, customer_demographics, household_demographics, store_returns
where cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and sr_cdemo_sk = cd_demo_sk