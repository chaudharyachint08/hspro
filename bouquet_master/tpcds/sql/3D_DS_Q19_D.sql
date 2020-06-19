select *
from store_sales, item, customer, store
where ss_item_sk = i_item_sk and ss_store_sk = s_store_sk and ss_customer_sk = c_customer_sk and i_manager_id=97 and ss_list_price <= 17.5