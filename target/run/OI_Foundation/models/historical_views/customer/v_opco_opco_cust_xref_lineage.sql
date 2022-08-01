
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_opco_cust_xref_lineage 
  
   as (
    with v_opco_opco_cust_xref_lineage as(
    
    

        (
            select

                
                    cast("OPCO_OPCO_CUST_XREF_CK" as character varying(32)) as "OPCO_OPCO_CUST_XREF_CK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("OPCO_CUST_CODE_SK" as character varying(32)) as "OPCO_CUST_CODE_SK" ,
                    cast("OPCO_CUST_TYPE_SK" as character varying(32)) as "OPCO_CUST_TYPE_SK" ,
                    cast("OPCO_COMMSN_GRP_SK" as character varying(32)) as "OPCO_COMMSN_GRP_SK" ,
                    cast("OPCO_CUST_GRP_SK" as character varying(32)) as "OPCO_CUST_GRP_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("LINE_DISC_TXT" as character varying(60)) as "LINE_DISC_TXT" ,
                    cast("MAX_CR_AMT" as NUMBER(28,12)) as "MAX_CR_AMT" ,
                    cast("MANDATORY_CR_LMT_IND" as NUMBER(38,0)) as "MANDATORY_CR_LMT_IND" ,
                    cast("BLOCK_LVL_CD" as NUMBER(38,0)) as "BLOCK_LVL_CD" ,
                    cast("CUST_CLSSFCTN_CD" as character varying(20)) as "CUST_CLSSFCTN_CD" ,
                    cast("PYMNT_SCHD_DESC" as character varying(180)) as "PYMNT_SCHD_DESC" ,
                    cast("SLS_GRP_NM" as character varying(20)) as "SLS_GRP_NM" ,
                    cast("RGNL_SLS_MGR_NM" as character varying(20)) as "RGNL_SLS_MGR_NM" ,
                    cast("INVOICE_ACCT_NBR" as character varying(40)) as "INVOICE_ACCT_NBR" ,
                    cast("SEGMENT_CD" as character varying(40)) as "SEGMENT_CD" ,
                    cast("SEGMENTATION_TXT" as character varying(60)) as "SEGMENTATION_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_opco_cust_xref

            
        )

        
)

select * from v_opco_opco_cust_xref_lineage
  );
