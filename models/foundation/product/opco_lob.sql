with opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from {{ ref('v_opco_lob_lineage') }}
),
final as(
    select 
    opco_lob_sk, opco_lob_ck, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_lob_cd, src_lob_nm, lob_wo_spcl_chr_cd, actv_ind
    from opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final