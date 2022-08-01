with ax_opco_trans_status as(
    select *
    from {{ ref('ax_opco_trans_status') }}
)

select * from v_ax_opco_trans_status

