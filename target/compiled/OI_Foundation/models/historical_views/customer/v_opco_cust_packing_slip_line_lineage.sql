with v_opco_cust_packing_slip_line_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_PACKING_SLIP_LINE_SK" as character varying(32)) as "OPCO_CUST_PACKING_SLIP_LINE_SK" ,
                    cast("OPCO_CUST_PACKING_SLIP_LINE_CK" as character varying(32)) as "OPCO_CUST_PACKING_SLIP_LINE_CK" ,
                    cast("OPCO_CUST_PACKING_SLIP_LINE_AK" as character varying(93)) as "OPCO_CUST_PACKING_SLIP_LINE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("PACKING_SLIP_ID" as character varying(40)) as "PACKING_SLIP_ID" ,
                    cast("INTRY_TRANS_ID" as character varying(40)) as "INTRY_TRANS_ID" ,
                    cast("OPCO_CUST_PACKING_SLIP_SK" as character varying(32)) as "OPCO_CUST_PACKING_SLIP_SK" ,
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("ORIG_SLS_ORDR_SK" as character varying(32)) as "ORIG_SLS_ORDR_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_INVTRY_REF_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_REF_TYPE_SK" ,
                    cast("SLS_UOM_SK" as character varying(32)) as "SLS_UOM_SK" ,
                    cast("INVTRY_UOM_SK" as character varying(32)) as "INVTRY_UOM_SK" ,
                    cast("LINE_NBR" as NUMBER(28,12)) as "LINE_NBR" ,
                    cast("CUST_ITEM_ID" as character varying(40)) as "CUST_ITEM_ID" ,
                    cast("ITEM_DESC" as character varying(2000)) as "ITEM_DESC" ,
                    cast("INVTRY_REF_ID" as character varying(40)) as "INVTRY_REF_ID" ,
                    cast("INVTRY_REF_TRANS_ID" as character varying(40)) as "INVTRY_REF_TRANS_ID" ,
                    cast("SHIP_DT" as TIMESTAMP_NTZ) as "SHIP_DT" ,
                    cast("DLVRY_COUNTY_NM" as character varying(60)) as "DLVRY_COUNTY_NM" ,
                    cast("DLVRY_STATE_NM" as character varying(20)) as "DLVRY_STATE_NM" ,
                    cast("DLVRY_COUNTRY_NM" as character varying(20)) as "DLVRY_COUNTRY_NM" ,
                    cast("PARTIAL_DLVRY_IND" as NUMBER(38,0)) as "PARTIAL_DLVRY_IND" ,
                    cast("DLVRY_TYPE_TXT" as character varying(16777216)) as "DLVRY_TYPE_TXT" ,
                    cast("SLS_GRP_TXT" as character varying(20)) as "SLS_GRP_TXT" ,
                    cast("PACKING_SLIP_LINE_DESC" as character varying(160)) as "PACKING_SLIP_LINE_DESC" ,
                    cast("SPCL_ITEM_IND" as NUMBER(38,0)) as "SPCL_ITEM_IND" ,
                    cast("SPCL_ITEM_NBR" as character varying(24)) as "SPCL_ITEM_NBR" ,
                    cast("GRP_NM" as character varying(30)) as "GRP_NM" ,
                    cast("PER_UNIT_QTY" as NUMBER(28,12)) as "PER_UNIT_QTY" ,
                    cast("INVTRY_UNIT_QTY" as NUMBER(28,12)) as "INVTRY_UNIT_QTY" ,
                    cast("PICKED_ORDR_QTY" as NUMBER(28,12)) as "PICKED_ORDR_QTY" ,
                    cast("PACKED_ORDR_QTY" as NUMBER(28,12)) as "PACKED_ORDR_QTY" ,
                    cast("RELEASED_QTY" as NUMBER(28,12)) as "RELEASED_QTY" ,
                    cast("REMAINING_QTY" as NUMBER(28,12)) as "REMAINING_QTY" ,
                    cast("PER_UNIT_SLS_PRICE_AMT" as NUMBER(28,12)) as "PER_UNIT_SLS_PRICE_AMT" ,
                    cast("OPCO_CURRENCY_PICKED_LINE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_PICKED_LINE_AMT" ,
                    cast("OPCO_CURRENCY_PACKED_LINE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_PACKED_LINE_AMT" ,
                    cast("ITEM_WEIGHT_AMT" as NUMBER(28,12)) as "ITEM_WEIGHT_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_packing_slip_line

            
        )

        
)

select * from v_opco_cust_packing_slip_line_lineage