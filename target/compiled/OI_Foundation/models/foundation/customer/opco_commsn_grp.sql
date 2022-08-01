with opco_commsn_grp as(
    select *,
    rank() over(partition by opco_commsn_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_commsn_grp_lineage
),
final as(
    select 
     opco_commsn_grp_sk, opco_commsn_grp_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_commsn_grp_cd, src_commsn_grp_desc, src_commsn_grp_type_txt 
    from opco_commsn_grp
    where rnk = 1 and delete_dtm is null
)

select * from final