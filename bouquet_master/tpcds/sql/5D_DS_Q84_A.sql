select *
from customer, customer_address, customer_demographics, household_demographics, income_band, store_returns
where c_current_addr_sk = ca_address_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and ib_income_band_sk = hd_income_band_sk and sr_cdemo_sk = cd_demo_sk and ib_lower_bound >= 52066 and ib_upper_bound <= 52066 + 50000 and ca_gmt_offset=-7