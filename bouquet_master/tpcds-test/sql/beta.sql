select sum(inv_quantity_on_hand)
from inventory, date_dim
where d_date_sk = inv_date_sk and d_date between cast ('2002-01-26' as date) and cast ('2002-03-26' as date)
