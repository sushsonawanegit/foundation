
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ns_opco_gl_trans_type_curr 
  
   as (
    with v_ns_opco_gl_trans_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_gl_trans_type
)

select * from v_ns_opco_gl_trans_type
  );
