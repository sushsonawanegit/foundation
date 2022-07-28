with opco_brand as(
    select *,
    rank() over(partition by opco_brand_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_brand_lineage') }}
),
final as(
    select 
    opco_brand_sk, opco_brand_ck, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_brand_cd, src_brand_nm, brand_wo_spcl_chr_cd, actv_ind
    from opco_brand
    where rnk = 1 and delete_dtm is null
)

select * from final