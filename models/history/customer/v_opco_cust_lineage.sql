with v_opco_cust_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cust')])}}
)

select * from v_opco_cust_lineage