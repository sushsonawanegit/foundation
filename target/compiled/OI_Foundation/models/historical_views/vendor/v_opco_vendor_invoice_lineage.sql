with v_opco_vendor_invoice_lineage as(
    
    

        (
            select

                
                    cast("OPCO_VENDOR_INVOICE_SK" as character varying(32)) as "OPCO_VENDOR_INVOICE_SK" ,
                    cast("OPCO_VENDOR_INVOICE_CK" as character varying(32)) as "OPCO_VENDOR_INVOICE_CK" ,
                    cast("OPCO_VENDOR_INVOICE_AK" as character varying(93)) as "OPCO_VENDOR_INVOICE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("INVOICE_NBR" as character varying(40)) as "INVOICE_NBR" ,
                    cast("INTERNAL_INVOICE_NBR" as character varying(40)) as "INTERNAL_INVOICE_NBR" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_PO_SK" as character varying(32)) as "OPCO_PO_SK" ,
                    cast("OPCO_VENDOR_SK" as character varying(32)) as "OPCO_VENDOR_SK" ,
                    cast("OPCO_INVOICE_VENDOR_SK" as character varying(32)) as "OPCO_INVOICE_VENDOR_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PO_TYPE_SK" as character varying(32)) as "OPCO_PO_TYPE_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("VENDOR_GRP_CD" as character varying(20)) as "VENDOR_GRP_CD" ,
                    cast("INVOICE_DT" as TIMESTAMP_NTZ) as "INVOICE_DT" ,
                    cast("DUE_DT" as TIMESTAMP_NTZ) as "DUE_DT" ,
                    cast("GL_VOUCHER_NBR" as character varying(40)) as "GL_VOUCHER_NBR" ,
                    cast("DOCUMENT_NBR" as character varying(40)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as TIMESTAMP_NTZ) as "DOCUMENT_DT" ,
                    cast("CASH_DSCNT_DT" as TIMESTAMP_NTZ) as "CASH_DSCNT_DT" ,
                    cast("DEFAULT_COUNTRY_NM" as character varying(20)) as "DEFAULT_COUNTRY_NM" ,
                    cast("PURCHASER_NM" as character varying(40)) as "PURCHASER_NM" ,
                    cast("INVOICE_QTY" as NUMBER(28,12)) as "INVOICE_QTY" ,
                    cast("INVOICE_WEIGHT_AMT" as NUMBER(28,12)) as "INVOICE_WEIGHT_AMT" ,
                    cast("INVOICE_VOL_AMT" as NUMBER(28,12)) as "INVOICE_VOL_AMT" ,
                    cast("RQSTD_BY_NM" as character varying(120)) as "RQSTD_BY_NM" ,
                    cast("EXCHG_RT" as NUMBER(28,12)) as "EXCHG_RT" ,
                    cast("PYMNT_ID" as character varying(400)) as "PYMNT_ID" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("ITEM_BUYER_GRP_ID" as character varying(20)) as "ITEM_BUYER_GRP_ID" ,
                    cast("FIXED_DUE_DT" as TIMESTAMP_NTZ) as "FIXED_DUE_DT" ,
                    cast("TRANS_CURRENCY_INVOICE_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_INVOICE_AMT" ,
                    cast("OPCO_CURRENCY_INVOICE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_INVOICE_AMT" ,
                    cast("TRANS_CURRENCY_INVOICE_ROUNDOFF_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_INVOICE_ROUNDOFF_AMT" ,
                    cast("TRANS_CURRENCY_MISC_CHRGS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_MISC_CHRGS_AMT" ,
                    cast("OPCO_CURRENCY_MISC_CHRGS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_MISC_CHRGS_AMT" ,
                    cast("TRANS_CURRENCY_CASH_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_CASH_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_LINE_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_LINE_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_BALANCE_SLS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_BALANCE_SLS_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_invoice

            
        )

        
)

select * from v_opco_vendor_invoice_lineage