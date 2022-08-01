
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_opco_po_type_curr 
  
   as (
    with v_ax_opco_po_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_type

)

select * from v_ax_opco_po_type
  );
