with v_opco_brand as(
    {{ union_relations(
        relations=[ref('ns_opco_brand')])}}
)

select * from v_opco_brand