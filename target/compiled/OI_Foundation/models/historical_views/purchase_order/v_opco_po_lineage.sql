with v_opco_po_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PO_SK" as character varying(32)) as "OPCO_PO_SK" ,
                    cast("OPCO_PO_CK" as character varying(32)) as "OPCO_PO_CK" ,
                    cast("OPCO_PO_AK" as character varying(52)) as "OPCO_PO_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("PO_ID" as character varying(40)) as "PO_ID" ,
                    cast("PO_NM" as character varying(120)) as "PO_NM" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_VENDOR_SK" as character varying(32)) as "OPCO_VENDOR_SK" ,
                    cast("INVOICE_VENDOR_SK" as character varying(32)) as "INVOICE_VENDOR_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_TRANS_STATUS_SK" as character varying(32)) as "OPCO_TRANS_STATUS_SK" ,
                    cast("OPCO_PO_TYPE_SK" as character varying(32)) as "OPCO_PO_TYPE_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("INTERCOMPANY_ORIG_SLS_ORDR_SK" as character varying(32)) as "INTERCOMPANY_ORIG_SLS_ORDR_SK" ,
                    cast("INTERCOMPANY_ORIG_CUST_SK" as character varying(32)) as "INTERCOMPANY_ORIG_CUST_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_PO_DOC_STATUS_SK" as character varying(32)) as "OPCO_PO_DOC_STATUS_SK" ,
                    cast("WAREHOUSE_SK" as character varying(32)) as "WAREHOUSE_SK" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_LOCN_SK" as character varying(32)) as "OPCO_LOCN_SK" ,
                    cast("DLVRY_DT" as TIMESTAMP_NTZ) as "DLVRY_DT" ,
                    cast("PO_CRT_DTM" as TIMESTAMP_NTZ) as "PO_CRT_DTM" ,
                    cast("PO_REQSTD_BY_NM" as character varying(120)) as "PO_REQSTD_BY_NM" ,
                    cast("VENDOR_GRP_CD" as character varying(60)) as "VENDOR_GRP_CD" ,
                    cast("DLVRY_VENDOR_NM" as character varying(120)) as "DLVRY_VENDOR_NM" ,
                    cast("VENDOR_REF_TXT" as character varying(120)) as "VENDOR_REF_TXT" ,
                    cast("ALIAS_NM" as character varying(60)) as "ALIAS_NM" ,
                    cast("ITEM_BUYER_GRP_ID" as character varying(20)) as "ITEM_BUYER_GRP_ID" ,
                    cast("PURCHASED_BY_TXT" as character varying(40)) as "PURCHASED_BY_TXT" ,
                    cast("TAXABLE_IND" as NUMBER(38,0)) as "TAXABLE_IND" ,
                    cast("DSCNT_PCT" as NUMBER(28,12)) as "DSCNT_PCT" ,
                    cast("CASH_DSCNT_PCT" as NUMBER(28,12)) as "CASH_DSCNT_PCT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po

            
        )

        
)

select * from v_opco_po_lineage