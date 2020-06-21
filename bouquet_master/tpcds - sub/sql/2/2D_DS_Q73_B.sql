select ss_ticket_number, ss_customer_sk, count(*) cnt
from store_sales, date_dim, store, household_demographics
where d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and hd_demo_sk = ss_hdemo_sk and d_dom <= 2 and hd_buy_potential = '> 10000' and hd_vehicle_count > 0 and d_year in (2000, 2000+1, 2000+2) and s_county in ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
group by ss_ticket_number, ss_customer_sk