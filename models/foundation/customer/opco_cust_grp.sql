with opco_cust_grp as(
    select *,
    rank() over(partition by opco_cust_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cust_grp_lineage') }}
),
final as(
    select 
    opco_cust_grp_sk, opco_cust_grp_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_cust_grp_cd, src_cust_grp_desc  
    from opco_cust_grp
    where rnk = 1 and delete_dtm is null
)

select * from final

