with opco_locn as(
    select *,
    rank() over(partition by opco_locn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_locn_lineage
),
final as(
    select 
    opco_locn_sk, opco_locn_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, addr_ln_1_txt, addr_ln_2_txt, addr_ln_3_txt, city_nm, state_nm, country_nm, zip_cd, county_nm
    from opco_locn
    where rnk = 1 and delete_dtm is null
)

select * from final