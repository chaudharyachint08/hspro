select distinct(i_product_name)
 from item i1
 where i_manufact_id between 707 and 707+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'goldenrod' or i_color = 'deep') and 
        (i_units = 'Case' or i_units = 'Bundle') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'honeydew' or i_color = 'beige') and
        (i_units = 'Unknown' or i_units = 'Gross') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'turquoise' or i_color = 'tomato') and
        (i_units = 'N/A' or i_units = 'Bunch') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'salmon' or i_color = 'dim') and
        (i_units = 'Box' or i_units = 'Pound') and
        (i_size = 'small' or i_size = 'petite')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'forest' or i_color = 'linen') and 
        (i_units = 'Lb' or i_units = 'Oz') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'snow' or i_color = 'wheat') and
        (i_units = 'Dozen' or i_units = 'Tsp') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'seashell' or i_color = 'burlywood') and
        (i_units = 'Dram' or i_units = 'Pallet') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'burnished' or i_color = 'purple') and
        (i_units = 'Tbl' or i_units = 'Gram') and
        (i_size = 'small' or i_size = 'petite')
        )))) > 0
 order by i_product_name
 limit 100

