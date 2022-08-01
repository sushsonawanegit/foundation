

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_type  as
      (with opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_type_lineage
),
final as(
    select 
    OPCO_TYPE_SK, OPCO_TYPE_CK, CRT_DTM, STG_LOAD_DTM, DELETE_DTM, src_sys_nm, src_type_cd, src_type_desc, type_wo_spcl_chr_cd, actv_ind
    from opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    