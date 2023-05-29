with s_named_entities as (

    select * from {{ ref('stg__gdelt__articles') }} as gdelt_articles

),

persons as (

    select distinct
        t.value as actor_name,
        'person' as actor_type
    
    from s_named_entities
    cross join lateral split_to_table(s_named_entities.persons, ';') as t

),

organizations as (

    select distinct
        t.value as actor_name,
        'organization' as actor_type
    
    from s_named_entities
    cross join lateral split_to_table(s_named_entities.organizations, ';') as t

),

final as (

    select * from persons
    union all
    select * from organizations

)

select * from final