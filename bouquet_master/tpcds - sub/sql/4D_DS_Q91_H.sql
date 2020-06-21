select *
from call_center, catalog_returns, customer, customer_address, household_demographics
where cr_call_center_sk = cc_call_center_sk and cr_returning_customer_sk = c_customer_sk and hd_demo_sk = c_current_hdemo_sk and ca_address_sk = c_current_addr_sk and hd_buy_potential like '5001-10000%' and ca_gmt_offset = -7