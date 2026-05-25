

select*
from {{ source ("json", "complaints_json") }}
limit 5