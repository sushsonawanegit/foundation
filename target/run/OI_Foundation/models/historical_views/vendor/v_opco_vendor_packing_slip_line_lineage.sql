
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_vendor_packing_slip_line_lineage 
  
   as (
    with v_opco_vendor_packing_slip_line_lineage as(
    
    

        (
            select

                
                    cast("OPCO_VENDOR_PACKING_SLIP_LINE_SK" as character varying(32)) as "OPCO_VENDOR_PACKING_SLIP_LINE_SK" ,
                    cast("OPCO_VENDOR_PACKING_SLIP_LINE_CK" as character varying(32)) as "OPCO_VENDOR_PACKING_SLIP_LINE_CK" ,
                    cast("OPCO_VENDOR_PACKING_SLIP_LINE_AK" as character varying(134)) as "OPCO_VENDOR_PACKING_SLIP_LINE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("PACKING_SLIP_ID" as character varying(40)) as "PACKING_SLIP_ID" ,
                    cast("INTERNAL_PACKING_SLIP_ID" as character varying(40)) as "INTERNAL_PACKING_SLIP_ID" ,
                    cast("INVTRY_TRANS_ID" as character varying(40)) as "INVTRY_TRANS_ID" ,
                    cast("OPCO_VENDOR_PACKING_SLIP_SK" as character varying(32)) as "OPCO_VENDOR_PACKING_SLIP_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PO_SK" as character varying(32)) as "OPCO_PO_SK" ,
                    cast("ORIG_PO_SK" as character varying(32)) as "ORIG_PO_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_INVTRY_REF_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_REF_TYPE_SK" ,
                    cast("LINE_NBR" as NUMBER(28,12)) as "LINE_NBR" ,
                    cast("DLVRY_DT" as TIMESTAMP_NTZ) as "DLVRY_DT" ,
                    cast("INVTRY_TRANS_DT" as TIMESTAMP_NTZ) as "INVTRY_TRANS_DT" ,
                    cast("INVTRY_REF_ID" as character varying(40)) as "INVTRY_REF_ID" ,
                    cast("VENDOR_ITEM_ID" as character varying(40)) as "VENDOR_ITEM_ID" ,
                    cast("ITEM_DESC" as character varying(2000)) as "ITEM_DESC" ,
                    cast("PARTIAL_DLVRY_IND" as NUMBER(38,0)) as "PARTIAL_DLVRY_IND" ,
                    cast("DEST_COUNTRY_NM" as character varying(20)) as "DEST_COUNTRY_NM" ,
                    cast("DEST_STATE_NM" as character varying(20)) as "DEST_STATE_NM" ,
                    cast("DEST_COUNTY_NM" as character varying(60)) as "DEST_COUNTY_NM" ,
                    cast("PER_UNIT_QTY" as NUMBER(28,12)) as "PER_UNIT_QTY" ,
                    cast("ORDR_QTY" as NUMBER(28,12)) as "ORDR_QTY" ,
                    cast("RECEIVED_QTY" as NUMBER(28,12)) as "RECEIVED_QTY" ,
                    cast("REMAINING_QTY" as NUMBER(28,12)) as "REMAINING_QTY" ,
                    cast("INVTRY_QTY" as NUMBER(28,12)) as "INVTRY_QTY" ,
                    cast("OPCO_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TRANS_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip_line

            
        )

        
)

select * from v_opco_vendor_packing_slip_line_lineage
  );
