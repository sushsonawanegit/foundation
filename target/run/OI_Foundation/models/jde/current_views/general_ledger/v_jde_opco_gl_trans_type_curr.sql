
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_jde_opco_gl_trans_type_curr 
  
   as (
    with v_jde_opco_gl_trans_type as(
    select *,
    rank() over(partition by opco_gl_trans_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans_type
),
final as(
    select 
    *
    from v_jde_opco_gl_trans_type
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
