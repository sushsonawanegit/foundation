with v_opco_invtry_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_INVTRY_TRANS_SK" as character varying(32)) as "OPCO_INVTRY_TRANS_SK" ,
                    cast("OPCO_INVTRY_TRANS_CK" as character varying(32)) as "OPCO_INVTRY_TRANS_CK" ,
                    cast("OPCO_INVTRY_TRANS_AK" as character varying(16777216)) as "OPCO_INVTRY_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("OPCO_VENDOR_SK" as character varying(32)) as "OPCO_VENDOR_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_INVTRY_TRANS_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_TRANS_TYPE_SK" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("OPCO_PICKING_LIST_SK" as character varying(32)) as "OPCO_PICKING_LIST_SK" ,
                    cast("OPCO_INVTRY_ISSUE_STATUS_SK" as character varying(32)) as "OPCO_INVTRY_ISSUE_STATUS_SK" ,
                    cast("OPCO_INVTRY_RCPT_STATUS_SK" as character varying(32)) as "OPCO_INVTRY_RCPT_STATUS_SK" ,
                    cast("INVTRY_TRANS_ID" as character varying(80)) as "INVTRY_TRANS_ID" ,
                    cast("PARENT_INVTRY_TRANS_ID" as character varying(80)) as "PARENT_INVTRY_TRANS_ID" ,
                    cast("INVTRY_RETURN_TRANS_ID" as character varying(80)) as "INVTRY_RETURN_TRANS_ID" ,
                    cast("INVTRY_TRANSFER_TRANS_ID" as character varying(80)) as "INVTRY_TRANSFER_TRANS_ID" ,
                    cast("REF_INVTRY_TRANS_ID" as character varying(80)) as "REF_INVTRY_TRANS_ID" ,
                    cast("TRANS_REF_ID" as character varying(80)) as "TRANS_REF_ID" ,
                    cast("ITEM_ROUTE_ID" as character varying(80)) as "ITEM_ROUTE_ID" ,
                    cast("ITEM_BOM_ID" as character varying(80)) as "ITEM_BOM_ID" ,
                    cast("ASSET_ID" as character varying(80)) as "ASSET_ID" ,
                    cast("PACKING_SLIP_ID" as character varying(80)) as "PACKING_SLIP_ID" ,
                    cast("PROJECT_ADJSTMT_REF_ID" as character varying(80)) as "PROJECT_ADJSTMT_REF_ID" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("TRANS_STATUS_DT" as TIMESTAMP_NTZ) as "TRANS_STATUS_DT" ,
                    cast("FINANCIAL_TRANS_DT" as TIMESTAMP_NTZ) as "FINANCIAL_TRANS_DT" ,
                    cast("FINANCIAL_CLOSE_DT" as TIMESTAMP_NTZ) as "FINANCIAL_CLOSE_DT" ,
                    cast("RQST_SHIP_DT" as TIMESTAMP_NTZ) as "RQST_SHIP_DT" ,
                    cast("CNFRM_SHIP_DT" as TIMESTAMP_NTZ) as "CNFRM_SHIP_DT" ,
                    cast("ITEM_EXPCTD_MOVEMENT_DT" as TIMESTAMP_NTZ) as "ITEM_EXPCTD_MOVEMENT_DT" ,
                    cast("INVTRY_QTY_REGISTERED_DT" as TIMESTAMP_NTZ) as "INVTRY_QTY_REGISTERED_DT" ,
                    cast("INVTRY_QTY_PICKED_DT" as TIMESTAMP_NTZ) as "INVTRY_QTY_PICKED_DT" ,
                    cast("INVOICE_NBR" as character varying(80)) as "INVOICE_NBR" ,
                    cast("VOUCHER_NBR" as character varying(80)) as "VOUCHER_NBR" ,
                    cast("INVTRY_UPDT_VOUCHER_NBR" as character varying(80)) as "INVTRY_UPDT_VOUCHER_NBR" ,
                    cast("PACKING_SLIP_RETURNED_ITEM_IND" as NUMBER(38,0)) as "PACKING_SLIP_RETURNED_ITEM_IND" ,
                    cast("INVOICE_RETURNED_ITEM_IND" as NUMBER(38,0)) as "INVOICE_RETURNED_ITEM_IND" ,
                    cast("INTERCOMPANY_TRANSFER_IND" as NUMBER(38,0)) as "INTERCOMPANY_TRANSFER_IND" ,
                    cast("INVTRY_ORDER_PRCS_NBR" as character varying(80)) as "INVTRY_ORDER_PRCS_NBR" ,
                    cast("INVTRY_ORDER_PRCS_TYPE_TXT" as character varying(16777216)) as "INVTRY_ORDER_PRCS_TYPE_TXT" ,
                    cast("TRANS_OPEN_TXT" as character varying(16777216)) as "TRANS_OPEN_TXT" ,
                    cast("INVTRY_TRANS_DIRECTION_TXT" as character varying(16777216)) as "INVTRY_TRANS_DIRECTION_TXT" ,
                    cast("TRANS_QTY" as NUMBER(28,12)) as "TRANS_QTY" ,
                    cast("SETTLED_QTY" as NUMBER(28,12)) as "SETTLED_QTY" ,
                    cast("POSTED_COST_AMT" as NUMBER(28,12)) as "POSTED_COST_AMT" ,
                    cast("ADJSTMT_COST_AMT" as NUMBER(28,12)) as "ADJSTMT_COST_AMT" ,
                    cast("OPERATIONS_COST_AMT" as NUMBER(28,12)) as "OPERATIONS_COST_AMT" ,
                    cast("SETTLED_COST_AMT" as NUMBER(28,12)) as "SETTLED_COST_AMT" ,
                    cast("STD_COST_AMT" as NUMBER(28,12)) as "STD_COST_AMT" ,
                    cast("INVTRY_UPDT_QTY_COST_AMT" as NUMBER(28,12)) as "INVTRY_UPDT_QTY_COST_AMT" ,
                    cast("INVTRY_UPDT_QTY_REVENUE_AMT" as NUMBER(28,12)) as "INVTRY_UPDT_QTY_REVENUE_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_trans

            
        )

        
)

select * from v_opco_invtry_trans_lineage