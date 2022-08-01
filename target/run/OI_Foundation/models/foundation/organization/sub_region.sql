

      create or replace  table OI_DATA_DEV_V2.FND_DEV_BKP.sub_region  as
      (with sub_region as(
    select *,
    rank() over(partition by sub_region_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_sub_region_lineage
),
final as(
    select 
    sub_region_sk, sub_region_ck, crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, sub_region_id, sub_region_nm, actv_ind
    from sub_region
    where rnk = 1 and delete_dtm is null
)

select * from final
      );
    