explain (costs, verbose, format xml) selectivity (d_date_sk = ss_sold_date_sk and d_year = 2010) (0.001, 0.023)
select * from  date_dim, store_sales where d_date_sk = ss_sold_date_sk and d_year = 2010;


explain (costs, verbose, format xml) select dt.d_year ,item.i_brand_id brand_id ,item.i_brand brand,sum(ss_ext_sales_price) ext_price
from  date_dim dt ,store_sales,item
where dt.d_date_sk = store_sales.ss_sold_date_sk and item.i_item_sk = store_sales.ss_item_sk and item.i_manufact_id  <= 3 and dt.d_moy  <= 4
group by dt.d_year,item.i_brand,item.i_brand_id
order by dt.d_year,ext_price desc,brand_id;