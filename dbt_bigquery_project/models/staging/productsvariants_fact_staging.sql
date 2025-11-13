select * 

from {{ source('raw_data', 'productvariants_raw')}}
