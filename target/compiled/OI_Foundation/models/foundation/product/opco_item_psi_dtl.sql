with opco_item_psi_dtl as(
    select *,
    rank() over(partition by opco_item_psi_dtl_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_item_psi_dtl_lineage
),
final as(
    select 
    opco_item_psi_dtl_sk, 
    opco_item_psi_dtl_ck, 
    crt_dtm, 
    stg_load_dtm, 
    delete_dtm,
    src_sys_nm, 
    opco_id, 
    src_item_cd, 
    psi_type_txt, 
    psi_price_dt, 
    psi_price_amt, 
    psi_price_unit_qty, 
    psi_dlvry_qty, 
    psi_line_dscnt_amt

    from opco_item_psi_dtl
    where rnk = 1 and delete_dtm is null
)

select * from final