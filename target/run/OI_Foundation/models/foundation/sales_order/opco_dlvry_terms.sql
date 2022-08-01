

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_dlvry_terms  as
      (with opco_dlvry_terms as(
    select *,
    rank() over(partition by opco_dlvry_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_dlvry_terms_lineage
),
final as(
    select 
    opco_dlvry_terms_sk, opco_dlvry_terms_ck, CRT_DTM, STG_LOAD_DTM, DELETE_DTM,
    src_sys_nm, src_dlvry_terms_cd, src_dlvry_terms_desc
    from opco_dlvry_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    