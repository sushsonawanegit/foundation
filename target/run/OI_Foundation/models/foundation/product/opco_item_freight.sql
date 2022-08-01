

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.opco_item_freight  as
      (with opco_item_freight as(
    select *,
    rank() over(partition by opco_item_freight_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_item_freight_lineage
),
final as(
    select 
    opco_item_freight_sk, 
    opco_item_freight_ck, 
    crt_dtm, 
    stg_load_dtm, 
    delete_dtm,
    src_sys_nm, 
    src_item_freight_cd, 
    opco_id, 
    src_item_freight_nm
    
    from opco_item_freight
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    