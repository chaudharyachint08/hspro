select *
from call_center, catalog_returns, date_dim, customer, customer_address
where cr_call_center_sk = cc_call_center_sk and cr_returned_date_sk = d_date_sk and cr_returning_customer_sk = c_customer_sk and ca_address_sk = c_current_addr_sk and d_year = 2000 and d_moy = 12 and ca_gmt_offset = -7