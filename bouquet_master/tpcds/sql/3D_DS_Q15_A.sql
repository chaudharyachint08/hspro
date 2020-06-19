select *
from catalog_sales, customer, customer_address, date_dim
where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and cs_sold_date_sk = d_date_sk and ca_gmt_offset = -7.0 and d_year = 1900 and cs_list_price <= 10.5