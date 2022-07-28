with v_opco_cust_grp_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cust_grp')])}}
)

select * from v_opco_cust_grp_lineage