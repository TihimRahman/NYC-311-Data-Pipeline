{% snapshot snap_complaint_type_lookup %}

{{
    config(
        target_schema='silver',
        unique_key='complaint_type',
        strategy='check',
        check_cols=['category', 'is_quality_of_life', 'is_emergency'],
        invalidate_hard_deletes=True
    )
}}

select
    complaint_type,
    category,
    is_quality_of_life,
    is_emergency
from {{ ref('complaint_type_lookup') }}

{% endsnapshot %}