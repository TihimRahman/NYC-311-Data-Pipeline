

select*
from {{ source ("json", "complaints_json") }}
