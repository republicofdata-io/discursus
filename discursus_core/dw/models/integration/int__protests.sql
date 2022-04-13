with s_protests as (

    select * from {{ ref('stg__airbyte__protest_groupings') }}

)

select * from s_protests