

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.region  as
      (with region as(
    select *,
    rank() over(partition by region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_region_lineage
),
final as(
    select 
    region_sk, region_ck, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, region_id, region_nm, actv_ind
    from region
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    