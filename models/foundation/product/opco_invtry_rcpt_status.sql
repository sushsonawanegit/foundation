with opco_invtry_rcpt_status as(
    select *
    from {{ ref('v_opco_invtry_rcpt_status_lineage') }}
)

select * from opco_invtry_rcpt_status