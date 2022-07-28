with opco_item_cvrg_grp as(
    select *,
    rank() over(partition by opco_item_cvrg_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_item_cvrg_grp_lineage') }}
),
final as(
    select 
    opco_item_cvrg_grp_sk, 
    opco_item_cvrg_grp_ck, 
    crt_dtm, 
    stg_load_dtm, 
    delete_dtm,
    src_sys_nm, 
    src_item_cvrg_grp_cd,
    opco_id, 
    src_item_cvrg_grp_desc
    from opco_item_cvrg_grp
    where rnk = 1 and delete_dtm is null
)

select * from final