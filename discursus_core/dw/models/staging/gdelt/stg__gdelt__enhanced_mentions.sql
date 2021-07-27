with source as (

    select * from {{ source('gdelt', 'gdelt_enhanced_mentions') }}

),

final as (

    select distinct
        lower(cast(mention_identifier as string)) as mention_url,

        lower(cast(page_name as string)) as page_name,
        lower(cast(file_name as string)) as file_name,
        lower(cast(page_title as string)) as page_title,
        lower(cast(page_description as string)) as page_description,
        lower(cast(keywords as string)) as keywords        

    from source

)

select * from final