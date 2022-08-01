with v_opco_commsn_grp_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_commsn_grp')])}}
)

select * from v_opco_commsn_grp_lineage