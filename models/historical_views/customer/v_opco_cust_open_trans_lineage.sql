with v_opco_cust_open_trans_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cust_open_trans')])}}
)

select * from v_opco_cust_open_trans_lineage