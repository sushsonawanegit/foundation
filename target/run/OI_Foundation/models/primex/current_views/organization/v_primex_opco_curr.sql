
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_primex_opco_curr 
  
   as (
    with v_primex_opco as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco
)

select * from v_primex_opco
  );
