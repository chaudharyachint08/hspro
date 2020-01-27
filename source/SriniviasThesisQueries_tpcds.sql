-- D_DS_Q7
select i_item_id, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price
from store_sales, customer_demographics, date_dim, item, promotion
where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_cdemo_sk = cd_demo_sk and ss_promo_sk = p_promo_sk and cd_gender = 'F' and cd_marital_status = 'M' and cd_education_status = 'College' and d_year = 2001 and ss_list_price <= 1.5



-- D_DS_Q15
select ca_zip, cs_sales_price
from catalog_sales, customer, customer_address, date_dim
where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and cs_sold_date_sk = d_date_sk and ca_gmt_offset = -7.0 and d_year = 1900 and cs_list_price <= 10.5



-- D_DS_Q18
select i_item_id, ca_country, ca_state, ca_county, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price, cs_net_profit, c_birth_year, cd1.cd_dep_count
from catalog_sales, date_dim, item, customer_demographics cd1, customer, customer_demographics cd2, customer_address
where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd1.cd_demo_sk and cs_bill_customer_sk = c_customer_sk and c_current_cdemo_sk = cd2.cd_demo_sk and c_current_addr_sk = ca_address_sk and cd1.cd_gender = 'F' and cd1.cd_education_status = '2 yr Degree' and c_birth_month in (10, 9, 7, 5, 1, 3) and d_year = 2001 and ca_gmt_offset = -7 and i_current_price <= 10



-- D_DS_Q19
select i_brand_id, i_brand, i_manufact_id, i_manufact, ss_ext_sales_price
from store_sales, date_dim, item, customer, customer_address, store
where d_date_sk = ss_sold_date_sk and ss_item_sk = i_item_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and i_manager_id=97 and d_moy=12 and d_year=2002 and ss_list_price <= 17.5



-- D_DS_Q21
select w_warehouse_name,i_item_id, sum(inv_quantity_on_hand)
from inventory, warehouse, item, date_dim
where i_item_sk = inv_item_sk and w_warehouse_sk = inv_warehouse_sk and d_date_sk = inv_date_sk and i_current_price between 0.99 and 1.49 and d_date between cast ('2002-01-26' as date) and cast ('2002-03-26' as date)
group by w_warehouse_name, i_item_id



-- D_DS_Q22
select i_product_name, i_brand, i_class, i_category, sum(inv_quantity_on_hand)
from inventory, warehouse, item, date_dim
where i_item_sk = inv_item_sk and w_warehouse_sk = inv_warehouse_sk and d_date_sk = inv_date_sk and d_year = 1998
group by i_product_name, i_brand, i_class, i_category



-- D_DS_Q26
select i_item_id, avg(cs_quantity), avg(cs_list_price), avg(cs_coupon_amt), avg(cs_sales_price)
from catalog_sales, customer_demographics, date_dim, item, promotion
where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cs_promo_sk = p_promo_sk and cd_gender = 'F' and cd_marital_status = 'U' and cd_education_status = 'Unknown' and (p_channel_email = 'N' or p_channel_event = 'N') and d_year = 2002 and i_current_price <= 10
group by i_item_id
order by i_item_id



-- D_DS_Q27
select *
from store_sales, date_dim, item, store, customer_demographics
where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_store_sk = s_store_sk and ss_cdemo_sk = cd_demo_sk and cd_gender = 'F' and cd_marital_status = 'D' and cd_education_status = 'Primary' and d_year = 2000 and s_state in ('TN') and i_current_price <= 15



-- D_DS_Q36
select i_item_id, i_item_desc, s_store_id, s_store_name, ss_quantity, sr_return_quantity, cs_quantity
from date_dim d1, date_dim d2, date_dim d3, store_sales, store_returns, catalog_sales, store, item
where d1.d_date_sk = ss_sold_date_sk and sr_returned_date_sk = d2.d_date_sk and cs_sold_date_sk = d3.d_date_sk and s_store_sk = ss_store_sk and i_item_sk = ss_item_sk and ss_customer_sk = sr_customer_sk and ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number and sr_customer_sk = cs_bill_customer_sk and sr_item_sk = cs_item_sk and d1.d_year = 1999 and d2.d_moy between 4 and 9 and d2.d_year = 2000 and d3.d_year in (2000,2001,2002,2003,2004,2005) and i_current_price <= 20



-- D_DS_Q37
select i_item_id, i_item_desc, i_current_price
from store_sales, date_dim, item, store
where d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and s_state in ('TN','TN','TN','TN','TN','TN','TN','TN') and d_year = 2001
group by i_item_id, i_item_desc, i_current_price
order by i_item_id



-- D_DS_Q40
select w_state, i_item_id, sum(cs_sales_price)
from catalog_sales, warehouse, item, date_dim
where i_item_sk = cs_item_sk and w_warehouse_sk = cs_warehouse_sk and d_date_sk = cs_sold_date_sk and i_current_price between 0.99 and 1.49 and d_date between (cast ('2001-01-17' as date)) and (cast ('2001-03-17' as date))
group by w_state,i_item_id
order by w_state,i_item_id



-- D_DS_Q53
select *
from item, store_sales, date_dim, store
where i_item_sk = ss_item_sk and d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and d_year in (2001) and i_category in ('Books', 'Children', 'Electronics') and i_class in ('personal', 'portable', 'reference', 'self-help') and i_brand in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')



-- D_DS_Q62
select sm_type, web_name,
sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end) as "30 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end ) as "31-60 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end) as "61-90 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end) as "91-120 days",
sum(case when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1 else 0 end) as ">120 days"
from web_sales, warehouse, ship_mode, web_site, date_dim
where d_date_sk = ws_ship_date_sk and w_warehouse_sk = ws_warehouse_sk and sm_ship_mode_sk = ws_ship_mode_sk and web_site_sk = ws_web_site_sk and d_year = 2001
group by sm_type, web_name
order by sm_type, web_name



-- D_DS_Q67
select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
from store_sales, date_dim, store, item
where d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and d_year=1999
group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id



-- D_DS_Q73
select ss_ticket_number, ss_customer_sk, count(*) cnt
from store_sales, date_dim, store, household_demographics
where d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and hd_demo_sk = ss_hdemo_sk and d_dom <= 2 and hd_buy_potential = '> 10000' and hd_vehicle_count > 0 and d_year in (2000, 2000+1, 2000+2) and s_county in ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
group by ss_ticket_number, ss_customer_sk



-- D_DS_Q84
select c_customer_id as customer_id, c_last_name , c_first_name
from customer, customer_address, customer_demographics, household_demographics, income_band, store_returns
where c_current_addr_sk = ca_address_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and ib_income_band_sk = hd_income_band_sk and sr_cdemo_sk = cd_demo_sk and ib_lower_bound >= 52066 and ib_upper_bound <= 52066 + 50000 and ca_gmt_offset=-7



-- D_DS_Q89
select i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum(ss_sales_price)
from item, store_sales, date_dim, store
where i_item_sk = ss_item_sk and d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and d_year in (1999) and i_category in ('Jewelry', 'Electronics', 'Music') and i_class in ('mens watch', 'wireless', 'classical')
group by i_category, i_class, i_brand, s_store_name, s_company_name, d_moy



-- D_DS_Q91
select cc_call_center_id, cc_name, cc_manager, sum(cr_net_loss)
from call_center, catalog_returns, date_dim, customer, customer_address, customer_demographics, household_demographics
where cr_call_center_sk = cc_call_center_sk and cr_returned_date_sk = d_date_sk and cr_returning_customer_sk = c_customer_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and ca_address_sk = c_current_addr_sk and d_year = 2000 and d_moy = 12 and ((cd_marital_status = 'M' and cd_education_status = 'Unknown') or (cd_marital_status = 'W' and cd_education_status = 'Advanced Degree')) and hd_buy_potential like '5001-10000%' and ca_gmt_offset = -7
group by cc_call_center_id, cc_name, cc_manager
order by sum(cr_net_loss) desc



-- D_DS_Q96
select s_store_name, hd_dep_count, ss_list_price, s_company_name
from store_sales, household_demographics, time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk and ss_hdemo_sk = hd_demo_sk and ss_store_sk = s_store_sk and t_hour = 8 and t_minute >= 30 and hd_dep_count = 2 and s_store_name = 'ese' and ss_list_price <= 19.5



-- D_DS_Q99
select sm_type, cc_name, count(*)
from catalog_sales, warehouse, ship_mode, call_center, date_dim
where d_date_sk = cs_ship_date_sk and w_warehouse_sk = cs_warehouse_sk and sm_ship_mode_sk = cs_ship_mode_sk and cc_call_center_sk = cs_call_center_sk and d_year = 2002
group by sm_type, cc_name
order by sm_type, cc_name