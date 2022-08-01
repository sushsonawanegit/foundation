with v_opco_cust_packing_slip_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_PACKING_SLIP_SK" as character varying(32)) as "OPCO_CUST_PACKING_SLIP_SK" ,
                    cast("OPCO_CUST_PACKING_SLIP_CK" as character varying(32)) as "OPCO_CUST_PACKING_SLIP_CK" ,
                    cast("OPCO_CUST_PACKING_SLIP_AK" as character varying(52)) as "OPCO_CUST_PACKING_SLIP_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("PACKING_SLIP_ID" as character varying(40)) as "PACKING_SLIP_ID" ,
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("OPCO_SLS_ORDR_TYPE_SK" as character varying(32)) as "OPCO_SLS_ORDR_TYPE_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("INVOICE_CUST_SK" as character varying(32)) as "INVOICE_CUST_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PICKING_LIST_SK" as character varying(32)) as "OPCO_PICKING_LIST_SK" ,
                    cast("OPCO_SITE_SK" as character varying(32)) as "OPCO_SITE_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("WAREHOUSE_SK" as character varying(32)) as "WAREHOUSE_SK" ,
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("DLVRY_LOCN_SK" as character varying(32)) as "DLVRY_LOCN_SK" ,
                    cast("GL_VOUCHER_NBR" as character varying(40)) as "GL_VOUCHER_NBR" ,
                    cast("PO_FORM_NBR" as character varying(40)) as "PO_FORM_NBR" ,
                    cast("SHIP_DT" as TIMESTAMP_NTZ) as "SHIP_DT" ,
                    cast("CUST_REF_TXT" as character varying(120)) as "CUST_REF_TXT" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("PRINT_INVOICE_IND" as NUMBER(1,0)) as "PRINT_INVOICE_IND" ,
                    cast("CREDIT_REASON_ID" as character varying(30)) as "CREDIT_REASON_ID" ,
                    cast("SPCL_INSTR_TXT" as character varying(1000)) as "SPCL_INSTR_TXT" ,
                    cast("REGISTERED_IND" as NUMBER(38,0)) as "REGISTERED_IND" ,
                    cast("DLVRY_CUST_NM" as character varying(120)) as "DLVRY_CUST_NM" ,
                    cast("INVOICE_CUST_NM" as character varying(120)) as "INVOICE_CUST_NM" ,
                    cast("INVOICE_ADDRESS_TXT" as character varying(500)) as "INVOICE_ADDRESS_TXT" ,
                    cast("LOAD_NBR" as character varying(40)) as "LOAD_NBR" ,
                    cast("PL_LOAD_CNT" as NUMBER(38,0)) as "PL_LOAD_CNT" ,
                    cast("MILES_CNT" as NUMBER(38,0)) as "MILES_CNT" ,
                    cast("DLVRY_QTY" as NUMBER(28,12)) as "DLVRY_QTY" ,
                    cast("PALLET_QTY" as NUMBER(28,12)) as "PALLET_QTY" ,
                    cast("PALLET_RATE_AMT" as NUMBER(28,12)) as "PALLET_RATE_AMT" ,
                    cast("DLVRY_VOLUME_AMT" as NUMBER(28,12)) as "DLVRY_VOLUME_AMT" ,
                    cast("DLVRY_WEIGHT_AMT" as NUMBER(28,12)) as "DLVRY_WEIGHT_AMT" ,
                    cast("DLVRY_COST_AMT" as NUMBER(28,12)) as "DLVRY_COST_AMT" ,
                    cast("PACKING_SLIP_AMT" as NUMBER(28,12)) as "PACKING_SLIP_AMT" ,
                    cast("INVOICE_NBR" as character varying(40)) as "INVOICE_NBR" ,
                    cast("INVOICE_DT" as TIMESTAMP_NTZ) as "INVOICE_DT" ,
                    cast("INVOICE_SENT_IND" as NUMBER(38,0)) as "INVOICE_SENT_IND" ,
                    cast("INVOICE_AMT" as NUMBER(28,12)) as "INVOICE_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_packing_slip

            
        )

        
)

select * from v_opco_cust_packing_slip_lineage