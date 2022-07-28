with v_opco_cash_dscnt_terms_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cash_dscnt_terms')])}}
)

select * from v_opco_cash_dscnt_terms_lineage