with opco_cust_type as(
    select *,
    rank() over(partition by opco_cust_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_cust_type_lineage') }}
),
final as(
    select 
    opco_cust_type_sk, opco_cust_type_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_cust_type_cd, src_cust_type_desc

    from opco_cust_type
    where rnk = 1 and delete_dtm is null
)

select * from final

