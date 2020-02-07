select *
from item, store_sales, date_dim, store
where i_item_sk = ss_item_sk and d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and d_year in (2001) and i_category in ('Books', 'Children', 'Electronics') and i_class in ('personal', 'portable', 'reference', 'self-help') and i_brand in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')