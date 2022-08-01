
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_lineage 
  
   as (
    with v_opco_cust_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("OPCO_CUST_CK" as character varying(32)) as "OPCO_CUST_CK" ,
                    cast("OPCO_CUST_AK" as character varying(52)) as "OPCO_CUST_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("CUST_ID" as character varying(40)) as "CUST_ID" ,
                    cast("CUST_NM" as character varying(120)) as "CUST_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("DBA_NM" as character varying(60)) as "DBA_NM" ,
                    cast("GOVT_ISSUED_ID" as character varying(1)) as "GOVT_ISSUED_ID" ,
                    cast("DUNS_NBR" as character varying(1)) as "DUNS_NBR" ,
                    cast("LOB_NM" as character varying(1)) as "LOB_NM" ,
                    cast("VAT_NUMBER" as character varying(40)) as "VAT_NUMBER" ,
                    cast("LANGUAGE_TXT" as character varying(1)) as "LANGUAGE_TXT" ,
                    cast("COUNTRY_CD" as character varying(20)) as "COUNTRY_CD" ,
                    cast("WEBSITE_URL_TXT" as character varying(510)) as "WEBSITE_URL_TXT" ,
                    cast("TAX_EXEMPT_NBR" as character varying(40)) as "TAX_EXEMPT_NBR" ,
                    cast("TAX_GRP_TXT" as character varying(22)) as "TAX_GRP_TXT" ,
                    cast("CUST_ITEM_GRP_CD" as character varying(20)) as "CUST_ITEM_GRP_CD" ,
                    cast("END_MARKET_NM" as character varying(30)) as "END_MARKET_NM" ,
                    cast("COLLECTOR_CD" as character varying(20)) as "COLLECTOR_CD" ,
                    cast("PARTY_ID" as character varying(40)) as "PARTY_ID" ,
                    cast("FLAG_ID" as NUMBER(38,0)) as "FLAG_ID" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust

            
        )

        
)

select * from v_opco_cust_lineage
  );
