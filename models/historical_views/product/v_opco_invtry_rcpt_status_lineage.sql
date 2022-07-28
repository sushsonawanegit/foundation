with v_opco_invtry_rcpt_status_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_invtry_rcpt_status')])}}
)

select * from v_opco_invtry_rcpt_status_lineage