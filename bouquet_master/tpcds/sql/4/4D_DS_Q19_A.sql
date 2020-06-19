select *
from store_sales, date_dim, item, customer, customer_address
where d_date_sk = ss_sold_date_sk and ss_item_sk = i_item_sk and ss_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and i_manager_id=97 and d_moy=12 and d_year=2002 and ss_list_price <= 17.5