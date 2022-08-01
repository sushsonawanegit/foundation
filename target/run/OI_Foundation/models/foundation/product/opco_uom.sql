

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_uom  as
      (with opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_uom_lineage
),
final as(
    select 
    opco_uom_sk, opco_uom_ck, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_uom_cd, src_uom_desc
    from opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    