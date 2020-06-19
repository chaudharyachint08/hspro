select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
from store_sales, date_dim, store, item
where d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and d_year=1999
group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id