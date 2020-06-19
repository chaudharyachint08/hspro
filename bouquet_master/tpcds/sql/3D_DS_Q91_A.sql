select *
from call_center, catalog_returns, date_dim, customer
where cr_call_center_sk = cc_call_center_sk and cr_returned_date_sk = d_date_sk and cr_returning_customer_sk = c_customer_sk and d_year = 2000 and d_moy = 12