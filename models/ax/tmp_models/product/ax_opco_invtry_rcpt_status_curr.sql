with ax_opco_invtry_rcpt_status as(
    select *
    from {{ ref('ax_opco_invtry_rcpt_status') }}
)

select * from ax_opco_invtry_rcpt_status