with v_opco_po_line_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PO_LINE_SK" as character varying(32)) as "OPCO_PO_LINE_SK" ,
                    cast("OPCO_PO_LINE_CK" as character varying(32)) as "OPCO_PO_LINE_CK" ,
                    cast("OPCO_PO_LINE_AK" as character varying(16777216)) as "OPCO_PO_LINE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("PO_ID" as character varying(40)) as "PO_ID" ,
                    cast("PO_LINE_NBR" as NUMBER(28,12)) as "PO_LINE_NBR" ,
                    cast("OPCO_PO_SK" as character varying(32)) as "OPCO_PO_SK" ,
                    cast("OPCO_VENDOR_SK" as character varying(32)) as "OPCO_VENDOR_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_TRANS_STATUS_SK" as character varying(32)) as "OPCO_TRANS_STATUS_SK" ,
                    cast("OPCO_PO_TYPE_SK" as character varying(32)) as "OPCO_PO_TYPE_SK" ,
                    cast("PURCHASE_UOM_SK" as character varying(32)) as "PURCHASE_UOM_SK" ,
                    cast("OPCO_GL_ACCT_SK" as character varying(32)) as "OPCO_GL_ACCT_SK" ,
                    cast("OPCO_INVTRY_REF_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_REF_TYPE_SK" ,
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("INVTRY_TRANS_ID" as character varying(40)) as "INVTRY_TRANS_ID" ,
                    cast("INVTRY_REF_ID" as character varying(40)) as "INVTRY_REF_ID" ,
                    cast("INVTRY_REF_TRANS_ID" as character varying(40)) as "INVTRY_REF_TRANS_ID" ,
                    cast("PROJECT_ITEM_TRANS_ID" as character varying(40)) as "PROJECT_ITEM_TRANS_ID" ,
                    cast("REQ_PLAN_SCHEDULE_ID" as character varying(60)) as "REQ_PLAN_SCHEDULE_ID" ,
                    cast("REQ_PLAN_ORDR_ID" as character varying(40)) as "REQ_PLAN_ORDR_ID" ,
                    cast("ASSET_ID" as character varying(40)) as "ASSET_ID" ,
                    cast("PROJECT_TAX_GRP_ID" as character varying(22)) as "PROJECT_TAX_GRP_ID" ,
                    cast("PO_LINE_NM" as character varying(2000)) as "PO_LINE_NM" ,
                    cast("PO_LINE_LOCKED_IND" as NUMBER(38,0)) as "PO_LINE_LOCKED_IND" ,
                    cast("PO_LINE_DLVRY_COMPLETE_IND" as NUMBER(38,0)) as "PO_LINE_DLVRY_COMPLETE_IND" ,
                    cast("SCRAP_IND" as NUMBER(38,0)) as "SCRAP_IND" ,
                    cast("TAXABLE_IND" as NUMBER(38,0)) as "TAXABLE_IND" ,
                    cast("TAX_ITEM_GRP_CD" as character varying(20)) as "TAX_ITEM_GRP_CD" ,
                    cast("RQST_DLVRY_DT" as TIMESTAMP_NTZ) as "RQST_DLVRY_DT" ,
                    cast("ACTL_DLVRY_DT" as TIMESTAMP_NTZ) as "ACTL_DLVRY_DT" ,
                    cast("DLVRY_TYPE_TXT" as character varying(16777216)) as "DLVRY_TYPE_TXT" ,
                    cast("PO_LINE_CRT_DTM" as TIMESTAMP_NTZ) as "PO_LINE_CRT_DTM" ,
                    cast("PO_LINE_CRT_BY" as character varying(10)) as "PO_LINE_CRT_BY" ,
                    cast("VENDOR_GRP_CD" as character varying(60)) as "VENDOR_GRP_CD" ,
                    cast("VENDOR_ITEM_ID" as character varying(40)) as "VENDOR_ITEM_ID" ,
                    cast("INVTRY_UNIT_ORDR_QTY" as NUMBER(28,12)) as "INVTRY_UNIT_ORDR_QTY" ,
                    cast("PURCHASE_UNIT_ORDR_QTY" as NUMBER(28,12)) as "PURCHASE_UNIT_ORDR_QTY" ,
                    cast("PURCHASE_UNIT_FINANCIAL_REMAINING_QTY" as NUMBER(28,12)) as "PURCHASE_UNIT_FINANCIAL_REMAINING_QTY" ,
                    cast("PURCHASE_UNIT_PHYSICAL_REMAINING_QTY" as NUMBER(28,12)) as "PURCHASE_UNIT_PHYSICAL_REMAINING_QTY" ,
                    cast("INVTRY_UNIT_PHYSICAL_REMAINING_QTY" as NUMBER(28,12)) as "INVTRY_UNIT_PHYSICAL_REMAINING_QTY" ,
                    cast("INVTRY_UNIT_FINANCIAL_REMAINING_QTY" as NUMBER(28,12)) as "INVTRY_UNIT_FINANCIAL_REMAINING_QTY" ,
                    cast("PURCHASE_UNIT_RECEIVED_QTY" as NUMBER(28,12)) as "PURCHASE_UNIT_RECEIVED_QTY" ,
                    cast("INVTRY_UNIT_RECEIVED_QTY" as NUMBER(28,12)) as "INVTRY_UNIT_RECEIVED_QTY" ,
                    cast("PER_UNIT_QTY" as NUMBER(28,12)) as "PER_UNIT_QTY" ,
                    cast("PO_LINE_DSCNT_PCT" as NUMBER(28,12)) as "PO_LINE_DSCNT_PCT" ,
                    cast("PER_UNIT_LINE_DSCNT_AMT" as NUMBER(28,12)) as "PER_UNIT_LINE_DSCNT_AMT" ,
                    cast("PER_UNIT_MULTI_LINE_DSCNT_AMT" as NUMBER(28,12)) as "PER_UNIT_MULTI_LINE_DSCNT_AMT" ,
                    cast("UNIT_PURCHASE_PRICE_AMT" as NUMBER(28,12)) as "UNIT_PURCHASE_PRICE_AMT" ,
                    cast("PO_LINE_AMT" as NUMBER(28,12)) as "PO_LINE_AMT" ,
                    cast("PURCHASE_MARKUP_AMT" as NUMBER(28,12)) as "PURCHASE_MARKUP_AMT" ,
                    cast("TAX_1099_AMT" as NUMBER(28,12)) as "TAX_1099_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_line

            
        )

        
)

select * from v_opco_po_line_lineage