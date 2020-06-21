select *
from store_sales, customer, customer_address, store
where ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and ss_list_price <= 17.5