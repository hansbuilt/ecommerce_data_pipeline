select *
from (
  select *,
         row_number() over (partition by id order by updated_at desc) as rn
from {{ source('raw_data', 'orders_raw')}}
) a
where rn = 1