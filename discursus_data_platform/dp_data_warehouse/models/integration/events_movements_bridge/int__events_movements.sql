with s_observations as (

    select * from {{ ref('int__events__3_associate_movements') }}

)

select * from s_observations
