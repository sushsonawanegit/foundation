with opco_dlvry_mode as(
    select *,
    rank() over(partition by opco_dlvry_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_dlvry_mode_lineage
),
final as(
    select 
    opco_dlvry_mode_sk, opco_dlvry_mode_ck, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, src_dlvry_mode_cd, src_dlvry_mode_desc
    from opco_dlvry_mode
    where rnk = 1 and delete_dtm is null
)

select * from final