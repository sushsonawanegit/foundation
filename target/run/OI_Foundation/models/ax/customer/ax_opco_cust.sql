begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust ("OPCO_CUST_SK", "OPCO_CUST_CK", "OPCO_CUST_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "CUST_ID", "CUST_NM", "OPCO_ID", "DBA_NM", "GOVT_ISSUED_ID", "DUNS_NBR", "LOB_NM", "VAT_NUMBER", "LANGUAGE_TXT", "COUNTRY_CD", "WEBSITE_URL_TXT", "TAX_EXEMPT_NBR", "TAX_GRP_TXT", "CUST_ITEM_GRP_CD", "END_MARKET_NM", "COLLECTOR_CD", "PARTY_ID", "FLAG_ID")
        (
            select "OPCO_CUST_SK", "OPCO_CUST_CK", "OPCO_CUST_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "CUST_ID", "CUST_NM", "OPCO_ID", "DBA_NM", "GOVT_ISSUED_ID", "DUNS_NBR", "LOB_NM", "VAT_NUMBER", "LANGUAGE_TXT", "COUNTRY_CD", "WEBSITE_URL_TXT", "TAX_EXEMPT_NBR", "TAX_GRP_TXT", "CUST_ITEM_GRP_CD", "END_MARKET_NM", "COLLECTOR_CD", "PARTY_ID", "FLAG_ID"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust__dbt_tmp
        );
    commit;