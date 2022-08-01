
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_vendor_invoice_line_lineage 
  
   as (
    with v_opco_vendor_invoice_line_lineage as(
    
    

        (
            select

                
                    cast("OPCO_VENDOR_INVOICE_LINE_SK" as character varying(32)) as "OPCO_VENDOR_INVOICE_LINE_SK" ,
                    cast("OPCO_VENDOR_INVOICE_LINE_CK" as character varying(32)) as "OPCO_VENDOR_INVOICE_LINE_CK" ,
                    cast("OPCO_VENDOR_INVOICE_LINE_AK" as character varying(134)) as "OPCO_VENDOR_INVOICE_LINE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("INVOICE_NBR" as character varying(40)) as "INVOICE_NBR" ,
                    cast("INTERNAL_INVOICE_NBR" as character varying(40)) as "INTERNAL_INVOICE_NBR" ,
                    cast("INVTRY_TRANS_ID" as character varying(40)) as "INVTRY_TRANS_ID" ,
                    cast("OPCO_VENDOR_INVOICE_SK" as character varying(32)) as "OPCO_VENDOR_INVOICE_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PO_SK" as character varying(32)) as "OPCO_PO_SK" ,
                    cast("ORIG_PO_SK" as character varying(32)) as "ORIG_PO_SK" ,
                    cast("GL_ACCT_SK" as character varying(32)) as "GL_ACCT_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_INVTRY_REF_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_REF_TYPE_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("LINE_NBR" as NUMBER(28,12)) as "LINE_NBR" ,
                    cast("INVOICE_DT" as TIMESTAMP_NTZ) as "INVOICE_DT" ,
                    cast("ITEM_DESC" as character varying(2000)) as "ITEM_DESC" ,
                    cast("VENDOR_ITEM_ID" as character varying(40)) as "VENDOR_ITEM_ID" ,
                    cast("INVTRY_REF_ID" as character varying(40)) as "INVTRY_REF_ID" ,
                    cast("INVTRY_REF_TRANS_ID" as character varying(40)) as "INVTRY_REF_TRANS_ID" ,
                    cast("INVTRY_TRANS_DT" as TIMESTAMP_NTZ) as "INVTRY_TRANS_DT" ,
                    cast("DEST_COUNTRY_NM" as character varying(20)) as "DEST_COUNTRY_NM" ,
                    cast("DEST_STATE_NM" as character varying(20)) as "DEST_STATE_NM" ,
                    cast("DEST_COUNTY_NM" as character varying(60)) as "DEST_COUNTY_NM" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("TAX_ITEM_GRP_ID" as character varying(20)) as "TAX_ITEM_GRP_ID" ,
                    cast("PER_UNIT_QTY" as NUMBER(28,12)) as "PER_UNIT_QTY" ,
                    cast("PURCHASE_QTY" as NUMBER(28,12)) as "PURCHASE_QTY" ,
                    cast("RECEIVED_QTY" as NUMBER(28,12)) as "RECEIVED_QTY" ,
                    cast("INVTRY_QTY" as NUMBER(28,12)) as "INVTRY_QTY" ,
                    cast("DSCNT_PCT" as NUMBER(28,12)) as "DSCNT_PCT" ,
                    cast("LINE_DSCNT_PCT" as NUMBER(28,12)) as "LINE_DSCNT_PCT" ,
                    cast("PER_UNIT_PRICE_AMT" as NUMBER(28,12)) as "PER_UNIT_PRICE_AMT" ,
                    cast("TRANS_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_TRANS_AMT" ,
                    cast("OPCO_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TRANS_AMT" ,
                    cast("TRANS_CURRENCY_MISC_CHRGS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_MISC_CHRGS_AMT" ,
                    cast("TRANS_CURRENCY_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_LINE_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_LINE_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_MULTI_LINE_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_MULTI_LINE_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_TAX_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_TAX_AMT" ,
                    cast("OPCO_CURRENCY_TAX_1099_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TAX_1099_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_invoice_line

            
        )

        
)

select * from v_opco_vendor_invoice_line_lineage
  );
