with opco_item_class as(
    select *,
    rank() over(partition by opco_item_class_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_item_class_lineage') }}
),
final as(
    select 
    opco_item_class_sk, 
    opco_item_class_ck, 
    crt_dtm, 
    stg_load_dtm, 
    delete_dtm,src_sys_nm, 
    src_ITEM_CLASS_cd, 
    src_ITEM_CLASS_desc
    from opco_item_class
    where rnk = 1 and delete_dtm is null
)

select * from final