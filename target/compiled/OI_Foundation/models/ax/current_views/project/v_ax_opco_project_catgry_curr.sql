with v_ax_opco_project_catgry as(
    select *,
    rank() over(partition by opco_project_catgry_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_catgry
),
final as(
    select 
    *
    from v_ax_opco_project_catgry
    where rnk = 1 and delete_dtm is null
)

select * from final