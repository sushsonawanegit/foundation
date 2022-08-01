with v_ax_opco_commsn_grp as(
    select *,
    rank() over(partition by opco_commsn_grp_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_commsn_grp
),
final as(
    select 
    *
    from v_ax_opco_commsn_grp
    where rnk = 1 and delete_dtm is null
)

select * from final