

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_purpose  as
      (with opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_purpose_lineage
),
final as(
    select 
    OPCO_PURPOSE_SK, OPCO_PURPOSE_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, src_sys_nm, src_purpose_cd, src_purpose_desc, purpose_wo_spcl_chr_cd, actv_ind
    from opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    