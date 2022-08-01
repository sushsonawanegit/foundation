
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_ax_opco_vendor_packing_slip_line_curr 
  
   as (
    with v_ax_opco_vendor_packing_slip_line as(
    select *,
    rank() over(partition by opco_vendor_packing_slip_line_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip_line
),
final as(
    select 
    *
    from v_ax_opco_vendor_packing_slip_line 
    where rnk = 1 and delete_dtm is null
)

select * from final
  );
