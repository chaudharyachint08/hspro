select *
from customer, customer_address, customer_demographics, household_demographics, store_returns
where c_current_addr_sk = ca_address_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and sr_cdemo_sk = cd_demo_sk and ca_gmt_offset=-7