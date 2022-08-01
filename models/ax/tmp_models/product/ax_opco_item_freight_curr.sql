with ax_opco_item_freight as(
    select *,
    rank() over(partition by opco_item_freight_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('ax_opco_item_freight') }}
),
final as(
    select 
    *    
    from ax_opco_item_freight
    where rnk = 1 and delete_dtm is null
)

select * from final