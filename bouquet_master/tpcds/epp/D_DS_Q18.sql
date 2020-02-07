select i_item_id, ca_country, ca_state, ca_county, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price, cs_net_profit, c_birth_year, cd1.cd_dep_count
from catalog_sales, date_dim, item, customer_demographics cd1, customer, customer_demographics cd2, customer_address
where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd1.cd_demo_sk and cs_bill_customer_sk = c_customer_sk and c_current_cdemo_sk = cd2.cd_demo_sk and c_current_addr_sk = ca_address_sk and cd1.cd_gender = 'F' and cd1.cd_education_status = '2 yr Degree' and c_birth_month in (10, 9, 7, 5, 1, 3) and d_year = 2001 and ca_gmt_offset = -7 and i_current_price <= 10