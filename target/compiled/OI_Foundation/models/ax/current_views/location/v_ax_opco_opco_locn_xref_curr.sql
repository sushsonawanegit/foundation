with v_ax_opco_opco_locn_xref as(
    select *,
    rank() over(partition by opco_sk, opco_locn_sk, opco_opco_locn_asscn_type_txt order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_opco_locn_xref
),
final as(
    select 
    *
    from v_ax_opco_opco_locn_xref
    where rnk = 1 and delete_dtm is null
)

select * from final