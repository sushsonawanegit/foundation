with v_opco as(
    
    

        (
            select

                
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CK" as character varying(32)) as "OPCO_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("OPCO_AK" as character varying(255)) as "OPCO_AK" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("OPCO_NM" as character varying(16777216)) as "OPCO_NM" ,
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("REGION_SK" as character varying(32)) as "REGION_SK" ,
                    cast("SUB_REGION_SK" as character varying(32)) as "SUB_REGION_SK" ,
                    cast("DBA_NM" as character varying(120)) as "DBA_NM" ,
                    cast("ENTERPRISE_IND" as NUMBER(1,0)) as "ENTERPRISE_IND" ,
                    cast("GOVT_ISSUED_ID" as character varying(130)) as "GOVT_ISSUED_ID" ,
                    cast("DUNS_NBR" as character varying(15)) as "DUNS_NBR" ,
                    cast("VAT_NBR" as character varying(40)) as "VAT_NBR" ,
                    cast("ACQSTN_DT" as DATE) as "ACQSTN_DT" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("INACTV_DT" as DATE) as "INACTV_DT" ,
                    cast("IMPORT_VAT_NBR" as character varying(40)) as "IMPORT_VAT_NBR" ,
                    cast("WEBSITE_URL_TXT" as character varying(510)) as "WEBSITE_URL_TXT" ,
                    cast("LANGUAGE_TXT" as character varying(20)) as "LANGUAGE_TXT" ,
                    cast("CONVERSION_DT" as DATE) as "CONVERSION_DT" ,
                    cast("FOREIGN_ENTY_IND" as NUMBER(1,0)) as "FOREIGN_ENTY_IND" ,
                    cast("COMBINED_FED_STATE_TAX_FILER_IND" as NUMBER(1,0)) as "COMBINED_FED_STATE_TAX_FILER_IND" ,
                    cast("VALIDATE_1099_ON_ENTRY_IND" as NUMBER(1,0)) as "VALIDATE_1099_ON_ENTRY_IND" ,
                    cast("PLANNING_COMPANY_IND" as NUMBER(1,0)) as "PLANNING_COMPANY_IND" ,
                    cast("SLS_TAX_GLOBAL_ID" as character varying(60)) as "SLS_TAX_GLOBAL_ID" ,
                    cast("SLS_TAX_LOCAL_ID" as character varying(60)) as "SLS_TAX_LOCAL_ID" ,
                    cast("PRODUCT_LINE_TXT" as character varying(20)) as "PRODUCT_LINE_TXT" ,
                    cast("PRNT_OPCO_ID" as character varying(50)) as "PRNT_OPCO_ID" ,
                    cast("COLLECTION_TYPE_TXT" as character varying(50)) as "COLLECTION_TYPE_TXT" ,
                    cast("DASHBOARD_USAGE_ID" as NUMBER(1,0)) as "DASHBOARD_USAGE_ID" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CK" as character varying(32)) as "OPCO_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("OPCO_AK" as character varying(255)) as "OPCO_AK" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("OPCO_NM" as character varying(16777216)) as "OPCO_NM" ,
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("REGION_SK" as character varying(32)) as "REGION_SK" ,
                    cast("SUB_REGION_SK" as character varying(32)) as "SUB_REGION_SK" ,
                    cast("DBA_NM" as character varying(120)) as "DBA_NM" ,
                    cast("ENTERPRISE_IND" as NUMBER(1,0)) as "ENTERPRISE_IND" ,
                    cast("GOVT_ISSUED_ID" as character varying(130)) as "GOVT_ISSUED_ID" ,
                    cast("DUNS_NBR" as character varying(15)) as "DUNS_NBR" ,
                    cast("VAT_NBR" as character varying(40)) as "VAT_NBR" ,
                    cast("ACQSTN_DT" as DATE) as "ACQSTN_DT" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("INACTV_DT" as DATE) as "INACTV_DT" ,
                    cast("IMPORT_VAT_NBR" as character varying(40)) as "IMPORT_VAT_NBR" ,
                    cast("WEBSITE_URL_TXT" as character varying(510)) as "WEBSITE_URL_TXT" ,
                    cast("LANGUAGE_TXT" as character varying(20)) as "LANGUAGE_TXT" ,
                    cast("CONVERSION_DT" as DATE) as "CONVERSION_DT" ,
                    cast("FOREIGN_ENTY_IND" as NUMBER(1,0)) as "FOREIGN_ENTY_IND" ,
                    cast("COMBINED_FED_STATE_TAX_FILER_IND" as NUMBER(1,0)) as "COMBINED_FED_STATE_TAX_FILER_IND" ,
                    cast("VALIDATE_1099_ON_ENTRY_IND" as NUMBER(1,0)) as "VALIDATE_1099_ON_ENTRY_IND" ,
                    cast("PLANNING_COMPANY_IND" as NUMBER(1,0)) as "PLANNING_COMPANY_IND" ,
                    cast("SLS_TAX_GLOBAL_ID" as character varying(60)) as "SLS_TAX_GLOBAL_ID" ,
                    cast("SLS_TAX_LOCAL_ID" as character varying(60)) as "SLS_TAX_LOCAL_ID" ,
                    cast("PRODUCT_LINE_TXT" as character varying(20)) as "PRODUCT_LINE_TXT" ,
                    cast("PRNT_OPCO_ID" as character varying(50)) as "PRNT_OPCO_ID" ,
                    cast("COLLECTION_TYPE_TXT" as character varying(50)) as "COLLECTION_TYPE_TXT" ,
                    cast("DASHBOARD_USAGE_ID" as NUMBER(1,0)) as "DASHBOARD_USAGE_ID" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CK" as character varying(32)) as "OPCO_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("OPCO_AK" as character varying(255)) as "OPCO_AK" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("OPCO_NM" as character varying(16777216)) as "OPCO_NM" ,
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("REGION_SK" as character varying(32)) as "REGION_SK" ,
                    cast("SUB_REGION_SK" as character varying(32)) as "SUB_REGION_SK" ,
                    cast("DBA_NM" as character varying(120)) as "DBA_NM" ,
                    cast("ENTERPRISE_IND" as NUMBER(1,0)) as "ENTERPRISE_IND" ,
                    cast("GOVT_ISSUED_ID" as character varying(130)) as "GOVT_ISSUED_ID" ,
                    cast("DUNS_NBR" as character varying(15)) as "DUNS_NBR" ,
                    cast("VAT_NBR" as character varying(40)) as "VAT_NBR" ,
                    cast("ACQSTN_DT" as DATE) as "ACQSTN_DT" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("INACTV_DT" as DATE) as "INACTV_DT" ,
                    cast("IMPORT_VAT_NBR" as character varying(40)) as "IMPORT_VAT_NBR" ,
                    cast("WEBSITE_URL_TXT" as character varying(510)) as "WEBSITE_URL_TXT" ,
                    cast("LANGUAGE_TXT" as character varying(20)) as "LANGUAGE_TXT" ,
                    cast("CONVERSION_DT" as DATE) as "CONVERSION_DT" ,
                    cast("FOREIGN_ENTY_IND" as NUMBER(1,0)) as "FOREIGN_ENTY_IND" ,
                    cast("COMBINED_FED_STATE_TAX_FILER_IND" as NUMBER(1,0)) as "COMBINED_FED_STATE_TAX_FILER_IND" ,
                    cast("VALIDATE_1099_ON_ENTRY_IND" as NUMBER(1,0)) as "VALIDATE_1099_ON_ENTRY_IND" ,
                    cast("PLANNING_COMPANY_IND" as NUMBER(1,0)) as "PLANNING_COMPANY_IND" ,
                    cast("SLS_TAX_GLOBAL_ID" as character varying(60)) as "SLS_TAX_GLOBAL_ID" ,
                    cast("SLS_TAX_LOCAL_ID" as character varying(60)) as "SLS_TAX_LOCAL_ID" ,
                    cast("PRODUCT_LINE_TXT" as character varying(20)) as "PRODUCT_LINE_TXT" ,
                    cast("PRNT_OPCO_ID" as character varying(50)) as "PRNT_OPCO_ID" ,
                    cast("COLLECTION_TYPE_TXT" as character varying(50)) as "COLLECTION_TYPE_TXT" ,
                    cast("DASHBOARD_USAGE_ID" as NUMBER(1,0)) as "DASHBOARD_USAGE_ID" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CK" as character varying(32)) as "OPCO_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("OPCO_AK" as character varying(255)) as "OPCO_AK" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("OPCO_NM" as character varying(16777216)) as "OPCO_NM" ,
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast(null as character varying(32)) as "REGION_SK" ,
                    cast(null as character varying(32)) as "SUB_REGION_SK" ,
                    cast(null as character varying(120)) as "DBA_NM" ,
                    cast(null as NUMBER(1,0)) as "ENTERPRISE_IND" ,
                    cast(null as character varying(130)) as "GOVT_ISSUED_ID" ,
                    cast(null as character varying(15)) as "DUNS_NBR" ,
                    cast(null as character varying(40)) as "VAT_NBR" ,
                    cast("ACQSTN_DT" as DATE) as "ACQSTN_DT" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("INACTV_DT" as DATE) as "INACTV_DT" ,
                    cast(null as character varying(40)) as "IMPORT_VAT_NBR" ,
                    cast(null as character varying(510)) as "WEBSITE_URL_TXT" ,
                    cast(null as character varying(20)) as "LANGUAGE_TXT" ,
                    cast(null as DATE) as "CONVERSION_DT" ,
                    cast(null as NUMBER(1,0)) as "FOREIGN_ENTY_IND" ,
                    cast(null as NUMBER(1,0)) as "COMBINED_FED_STATE_TAX_FILER_IND" ,
                    cast(null as NUMBER(1,0)) as "VALIDATE_1099_ON_ENTRY_IND" ,
                    cast(null as NUMBER(1,0)) as "PLANNING_COMPANY_IND" ,
                    cast(null as character varying(60)) as "SLS_TAX_GLOBAL_ID" ,
                    cast(null as character varying(60)) as "SLS_TAX_LOCAL_ID" ,
                    cast(null as character varying(20)) as "PRODUCT_LINE_TXT" ,
                    cast(null as character varying(50)) as "PRNT_OPCO_ID" ,
                    cast(null as character varying(50)) as "COLLECTION_TYPE_TXT" ,
                    cast(null as NUMBER(1,0)) as "DASHBOARD_USAGE_ID" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco

            
        )

        
)

select * from v_opco