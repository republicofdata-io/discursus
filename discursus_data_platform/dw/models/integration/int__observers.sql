with s_articles as (

  select * from {{ ref('int__articles') }}

),

final as (

  select distinct
    split(split(article_url, '//')[1], '/')[0]::string as observer_domain

  from s_articles

)

select * from final
