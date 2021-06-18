with s_gdelt_events as (

    select * from {{ ref('stg__gdelt__events') }}

)

select * from s_gdelt_events