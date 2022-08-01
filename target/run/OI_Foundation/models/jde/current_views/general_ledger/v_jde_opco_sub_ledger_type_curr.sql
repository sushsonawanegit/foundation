
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_jde_opco_sub_ledger_type_curr 
  
   as (
    with v_jde_opco_sub_ledger_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_sub_ledger_type
)

select * from v_jde_opco_sub_ledger_type
  );
