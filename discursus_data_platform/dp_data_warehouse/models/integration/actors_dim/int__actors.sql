with s_named_entities as (

    select * from {{ ref('stg__gdelt__mention_named_entities') }}

),

final as (

    select distinct
        entity_name as actor_name
    
    from s_named_entities

)

select * from final