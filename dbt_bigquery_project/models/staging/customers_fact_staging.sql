select * 

from {{ source('raw_data', 'customers_raw')}}
