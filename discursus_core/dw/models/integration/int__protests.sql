with s_protests as (

    select * from {{ ref('stg__stitch__protest_groupings') }}

)

select * from s_protests