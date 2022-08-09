with ax_fscl_clndr as(
    select *,
    rank() over(partition by fscl_clndr_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_fscl_clndr
),
final as(
    select 
    *
    from ax_fscl_clndr
    where rnk = 1 and delete_dtm is null
)

select * from final