with opco_item_invtry as(
    select *,
    rank() over(partition by opco_item_sk, opco_assctn_sk  order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_item_invtry_lineage') }}
),
final as(
    select 
    opco_item_sk, opco_assctn_sk, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, src_sys_nm, item_avbl_physical_qty, item_posted_qty, item_posted_value_amt
    from opco_item_invtry
    where rnk = 1 and delete_dtm is null
)

select * from final