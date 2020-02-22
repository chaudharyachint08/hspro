select i_item_id, sum(inv_quantity_on_hand)
from inventory, item, date_dim
where i_item_sk = inv_item_sk and d_date_sk = inv_date_sk and i_current_price between 0.99 and 1.49 and d_date between cast ('2002-01-26' as date) and cast ('2002-03-26' as date)
group by i_item_id
