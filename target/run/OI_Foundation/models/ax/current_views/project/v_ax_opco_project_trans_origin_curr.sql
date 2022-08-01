
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_opco_project_trans_origin_curr 
  
   as (
    with v_ax_opco_project_trans_origin as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_trans_origin

)

select * from v_ax_opco_project_trans_origin
  );
