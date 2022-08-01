with opco_item_model_grp as(
    select *,
    rank() over(partition by opco_item_model_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_item_model_grp_lineage
),
final as(
    select 
    opco_item_model_grp_sk, 
    opco_item_model_grp_ck, 
    crt_dtm, 
    stg_load_dtm, 
    delete_dtm,
    src_sys_nm, 
    src_item_model_grp_cd, 
    src_item_model_grp_nm
    from opco_item_model_grp
    where rnk = 1 and delete_dtm is null
)

select * from final