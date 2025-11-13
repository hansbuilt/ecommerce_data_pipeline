select * 

from {{ source('raw_data', 'products_raw')}}
