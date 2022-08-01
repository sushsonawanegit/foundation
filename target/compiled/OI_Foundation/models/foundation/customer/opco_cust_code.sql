with opco_cust_code as(
    select *,
    rank() over(partition by opco_cust_code_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_code_lineage
),
final as(
    select 
    opco_cust_code_sk, opco_cust_code_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_cust_code_cd, src_cust_code_desc
    from opco_cust_code
    where rnk = 1 and delete_dtm is null
)

select * from final