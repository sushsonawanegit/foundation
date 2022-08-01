
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ns_opco_gl_trans_curr 
  
   as (
    with v_ns_opco_gl_trans as(
    select *,
    rank() over(partition by opco_gl_trans_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_gl_trans
),
final as(
    select 
    *
    from v_ns_opco_gl_trans
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
