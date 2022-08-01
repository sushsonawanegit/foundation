

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_site  as
      (with opco_site as(
    select *,
    rank() over(partition by opco_site_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_site_lineage
),
final as(
    select 
    opco_site_sk, opco_site_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, src_site_id, src_site_nm, actv_ind  
    
    from opco_site
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    