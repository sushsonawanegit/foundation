with v_opco_fscl_yr_gl_opening_bal as(
    {{ union_relations(
        relations=[ref('ax_opco_fscl_yr_gl_opening_bal'), ref('ns_opco_fscl_yr_gl_opening_bal'), ref('jde_hnck_opco_fscl_yr_gl_opening_bal'), ref('syspro_primex_opco_fscl_yr_gl_opening_bal')])}}
)

select * from v_opco_fscl_yr_gl_opening_bal