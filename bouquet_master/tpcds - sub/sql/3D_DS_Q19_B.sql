select *
from store_sales, item, customer, customer_address
where ss_item_sk = i_item_sk and ss_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and i_manager_id=97 and ss_list_price <= 17.5