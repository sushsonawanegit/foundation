

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.warehouse  as
      (with warehouse as(
    select *,
    rank() over(partition by warehouse_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_warehouse_lineage
),
final as(
    select 
    warehouse_sk, warehouse_ck, crt_dtm, stg_load_dtm, delete_dtm,
    src_sys_nm, opco_id, warehouse_id, warehouse_nm, actv_ind 
    
    from warehouse
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    