with v_ax_opco_handling_status_curr as(
    select *
    from {{ ref('ax_opco_handling_status') }}
)

select * from v_ax_opco_handling_status_curr
