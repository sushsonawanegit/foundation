with v_opco_cust_settled_trans_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cust_settled_trans')])}}
)

select * from v_opco_cust_settled_trans_lineage