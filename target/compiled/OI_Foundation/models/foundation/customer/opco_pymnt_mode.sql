with opco_pymnt_mode as(
    select *,
    rank() over(partition by opco_pymnt_mode_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_pymnt_mode_lineage
),
final as(
    select 
    opco_pymnt_mode_sk, opco_pymnt_mode_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_pymnt_mode_cd, src_pymnt_mode_desc
    from opco_pymnt_mode
    where rnk = 1 and delete_dtm is null
)

select * from final