with v_opco_project_item_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_ITEM_TRANS_SK" as character varying(32)) as "OPCO_PROJECT_ITEM_TRANS_SK" ,
                    cast("OPCO_PROJECT_ITEM_TRANS_CK" as character varying(32)) as "OPCO_PROJECT_ITEM_TRANS_CK" ,
                    cast("OPCO_PROJECT_ITEM_TRANS_AK" as character varying(181)) as "OPCO_PROJECT_ITEM_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("PROJECT_ITEM_TRANS_ID" as character varying(80)) as "PROJECT_ITEM_TRANS_ID" ,
                    cast("INVTRY_TRANS_ID" as character varying(80)) as "INVTRY_TRANS_ID" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("TRANS_UOM_SK" as character varying(32)) as "TRANS_UOM_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_PROJECT_TRANS_STATUS_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_STATUS_SK" ,
                    cast("OPCO_PROJECT_TRANS_ORIGIN_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_ORIGIN_SK" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("TRANS_DESC" as character varying(4000)) as "TRANS_DESC" ,
                    cast("LINE_PROPERTY_ID" as character varying(80)) as "LINE_PROPERTY_ID" ,
                    cast("TAX_GRP_ID" as character varying(44)) as "TAX_GRP_ID" ,
                    cast("TAX_ITEM_GRP_ID" as character varying(40)) as "TAX_ITEM_GRP_ID" ,
                    cast("PACKING_SLIP_VOUCHER_NBR" as character varying(80)) as "PACKING_SLIP_VOUCHER_NBR" ,
                    cast("CUST_INVOICE_NBR" as character varying(80)) as "CUST_INVOICE_NBR" ,
                    cast("PROJECT_ADJSMT_REF_ID" as character varying(80)) as "PROJECT_ADJSMT_REF_ID" ,
                    cast("REF_PROJECT_ITEM_TRANS_ID" as character varying(80)) as "REF_PROJECT_ITEM_TRANS_ID" ,
                    cast("CALC_IND" as NUMBER(38,0)) as "CALC_IND" ,
                    cast("PROJECT_COST_TRANS_CONVERSION_IND" as NUMBER(38,0)) as "PROJECT_COST_TRANS_CONVERSION_IND" ,
                    cast("TRANS_QTY" as NUMBER(28,12)) as "TRANS_QTY" ,
                    cast("PER_UNIT_SLS_PRICE_AMT" as NUMBER(28,12)) as "PER_UNIT_SLS_PRICE_AMT" ,
                    cast("ITEM_PRICE_AMT" as NUMBER(28,12)) as "ITEM_PRICE_AMT" ,
                    cast("TOTAL_REVENUE_AMT" as NUMBER(28,12)) as "TOTAL_REVENUE_AMT" ,
                    cast("TOTAL_COST_AMT" as NUMBER(28,12)) as "TOTAL_COST_AMT" ,
                    cast("TOTAL_STD_COST_AMT" as NUMBER(28,12)) as "TOTAL_STD_COST_AMT" ,
                    cast("MARGIN_CONTRIBUTION_PCT" as NUMBER(28,12)) as "MARGIN_CONTRIBUTION_PCT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_item_trans

            
        )

        
)

select * from v_opco_project_item_trans_lineage