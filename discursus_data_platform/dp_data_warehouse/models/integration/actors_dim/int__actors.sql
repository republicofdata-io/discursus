{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'article_url'
    )
}}

with s_named_entities as (

    select * from {{ ref('stg__gdelt__articles') }} as gdelt_articles

    {% if is_incremental() %}
        where source_file_date >= (select max(article_source_date) from {{ this }})
    {% else %}
        where source_file_date >= dateadd(week, -52, current_date)
    {% endif %}

),

persons as (

    select
        article_url,
        source_file_date as article_source_date,
        row_number() over (partition by article_url order by t.seq) as actor_position,
        t.value::string as actor_name,
        'person' as actor_type
    
    from s_named_entities
    cross join lateral flatten(input => split(s_named_entities.persons, ';'), outer => true) as t

),

organizations as (

    select
        article_url,
        source_file_date as article_source_date,
        row_number() over (partition by article_url order by t.seq) as actor_position,
        t.value as actor_name,
        'organization' as actor_type
    
    from s_named_entities
    cross join lateral flatten(input => split(s_named_entities.organizations, ';'), outer => true) as t

),

final as (

    select * from persons
    union all
    select * from organizations

)

select * from final
where actor_position <= 5
order by article_source_date, article_url, actor_position