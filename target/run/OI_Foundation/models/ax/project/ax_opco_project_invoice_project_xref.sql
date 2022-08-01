begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_invoice_project_xref ("OPCO_PROJECT_SK", "INVOICE_PROJECT_SK", "OPCO_PROJECT_INVOICE_PROJECT_XREF_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "INVOICE_PROJECT_ID")
        (
            select "OPCO_PROJECT_SK", "INVOICE_PROJECT_SK", "OPCO_PROJECT_INVOICE_PROJECT_XREF_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "INVOICE_PROJECT_ID"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_invoice_project_xref__dbt_tmp
        );
    commit;