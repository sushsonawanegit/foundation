with v_opco_cust_code_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cust_code')])}}
)

select * from v_opco_cust_code_lineage