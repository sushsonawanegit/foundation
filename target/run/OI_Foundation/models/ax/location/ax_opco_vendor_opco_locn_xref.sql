begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_opco_locn_xref ("OPCO_VENDOR_OPCO_LOCN_XREF_CK", "OPCO_VENDOR_SK", "OPCO_LOCN_SK", "OPCO_VENDOR_OPCO_LOCN_ASSCN_TYPE_TXT", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "CONTACT_TYPE_TXT", "CONTACT_NM", "CONTACT_PH_NBR", "CONTACT_PH_EXTN_NBR", "CONTACT_CELL_NBR", "CONTACT_LOCAL_PH_NBR", "CONTACT_FAX_NBR", "CONTACT_TELEX_NBR", "CONTACT_EMAIL_ID", "LANGUAGE_TXT", "START_DT", "END_DT")
        (
            select "OPCO_VENDOR_OPCO_LOCN_XREF_CK", "OPCO_VENDOR_SK", "OPCO_LOCN_SK", "OPCO_VENDOR_OPCO_LOCN_ASSCN_TYPE_TXT", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "CONTACT_TYPE_TXT", "CONTACT_NM", "CONTACT_PH_NBR", "CONTACT_PH_EXTN_NBR", "CONTACT_CELL_NBR", "CONTACT_LOCAL_PH_NBR", "CONTACT_FAX_NBR", "CONTACT_TELEX_NBR", "CONTACT_EMAIL_ID", "LANGUAGE_TXT", "START_DT", "END_DT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_opco_locn_xref__dbt_tmp
        );
    commit;