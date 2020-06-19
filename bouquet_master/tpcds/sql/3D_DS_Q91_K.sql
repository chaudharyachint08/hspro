select *
from catalog_returns, date_dim, customer, household_demographics
where cr_returned_date_sk = d_date_sk and cr_returning_customer_sk = c_customer_sk and hd_demo_sk = c_current_hdemo_sk and d_year = 2000 and d_moy = 12 and hd_buy_potential like '5001-10000%'