{% snapshot snap_complaints_bronze %}

{{
    config(
        target_schema='silver',
        unique_key='complaint_id',
        strategy='check',
        check_cols=[
            'status',
            'closed_at',
            'resolution_description',
            'resolution_updated_at',
            'agency_code'
        ],
        invalidate_hard_deletes=False
    )
}}


select
    raw_data:unique_key::string                                  as complaint_id,
    raw_data:status::string                                      as status,
    try_to_timestamp_ntz(raw_data:closed_date::string)           as closed_at,
    raw_data:resolution_description::string                      as resolution_description,
    try_to_timestamp_ntz(
        raw_data:resolution_action_updated_date::string
    )                                                            as resolution_updated_at,
    raw_data:agency::string                                      as agency_code
from {{ source('nyc311', 'complaints_raw') }}
where raw_data:unique_key is not null

{% endsnapshot %}