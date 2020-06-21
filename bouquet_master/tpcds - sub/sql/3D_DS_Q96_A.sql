select *
from store_sales, household_demographics, time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = hd_demo_sk and ss_store_sk = s_store_sk and t_hour = 8 and t_minute >= 30 and hd_dep_count = 2 and s_store_name = 'ese' and ss_list_price <= 19.5