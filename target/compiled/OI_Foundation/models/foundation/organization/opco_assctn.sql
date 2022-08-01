with opco_assctn as(
    select *,
    rank() over(partition by opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_assctn_lineage
),
final as(
    select 
    opco_assctn_sk, opco_assctn_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_assctn_cd, opco_id, opco_sk, opco_site_sk, cost_center_sk, warehouse_sk, actv_ind  
    
    from opco_assctn
    where rnk = 1 and delete_dtm is null
)

select * from final