with s_observations as (

    select * from {{ ref('int__observations') }}

)

select * from s_observations
