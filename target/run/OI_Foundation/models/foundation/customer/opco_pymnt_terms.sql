

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_pymnt_terms  as
      (with opco_pymnt_terms as(
    select *,
    rank() over(partition by opco_pymnt_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_pymnt_terms_lineage
),
final as(
    select 
    opco_pymnt_terms_sk, opco_pymnt_terms_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_pymnt_terms_cd, src_pymnt_terms_desc
    from opco_pymnt_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    