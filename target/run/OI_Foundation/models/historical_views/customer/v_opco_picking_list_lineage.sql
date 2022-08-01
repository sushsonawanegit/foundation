
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_picking_list_lineage 
  
   as (
    with v_opco_picking_list_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PICKING_LIST_SK" as character varying(32)) as "OPCO_PICKING_LIST_SK" ,
                    cast("OPCO_PICKING_LIST_CK" as character varying(32)) as "OPCO_PICKING_LIST_CK" ,
                    cast("OPCO_PICKING_LIST_AK" as character varying(32)) as "OPCO_PICKING_LIST_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("PICKING_LIST_ID" as character varying(20)) as "PICKING_LIST_ID" ,
                    cast("SHPMNT_ID" as character varying(20)) as "SHPMNT_ID" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("INVOICE_CUST_SK" as character varying(32)) as "INVOICE_CUST_SK" ,
                    cast("OPCO_INVTRY_TRANS_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_TRANS_TYPE_SK" ,
                    cast("OPCO_HANDLING_STATUS_SK" as character varying(32)) as "OPCO_HANDLING_STATUS_SK" ,
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("OPCO_SITE_SK" as character varying(32)) as "OPCO_SITE_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("SRC_CRT_DT" as TIMESTAMP_NTZ) as "SRC_CRT_DT" ,
                    cast("TRANS_REF_ID" as character varying(40)) as "TRANS_REF_ID" ,
                    cast("HANDLING_TYPE_TXT" as character varying(16777216)) as "HANDLING_TYPE_TXT" ,
                    cast("START_DTM" as TIMESTAMP_NTZ) as "START_DTM" ,
                    cast("END_DTM" as TIMESTAMP_NTZ) as "END_DTM" ,
                    cast("ACTIVATION_DTM" as TIMESTAMP_NTZ) as "ACTIVATION_DTM" ,
                    cast("RQSTD_SHIP_DT" as TIMESTAMP_NTZ) as "RQSTD_SHIP_DT" ,
                    cast("ACTL_SHIP_DT" as TIMESTAMP_NTZ) as "ACTL_SHIP_DT" ,
                    cast("CUST_DLVRY_DT" as TIMESTAMP_NTZ) as "CUST_DLVRY_DT" ,
                    cast("SHPMNT_TYPE_TXT" as character varying(16777216)) as "SHPMNT_TYPE_TXT" ,
                    cast("DLVRY_NM" as character varying(120)) as "DLVRY_NM" ,
                    cast("CREDIT_REASON_ID" as character varying(30)) as "CREDIT_REASON_ID" ,
                    cast("PO_FORM_NBR" as character varying(40)) as "PO_FORM_NBR" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("PICKING_LIST_UPDT_IND" as NUMBER(38,0)) as "PICKING_LIST_UPDT_IND" ,
                    cast("CUST_REF_TXT" as character varying(120)) as "CUST_REF_TXT" ,
                    cast("PRINT_INVOICE_IND" as NUMBER(1,0)) as "PRINT_INVOICE_IND" ,
                    cast("ITEM_RESERVED_IND" as NUMBER(38,0)) as "ITEM_RESERVED_IND" ,
                    cast("PACKING_SLIP_AVAILABLE_IND" as NUMBER(38,0)) as "PACKING_SLIP_AVAILABLE_IND" ,
                    cast("DLVRY_WINDOW_TXT" as character varying(40)) as "DLVRY_WINDOW_TXT" ,
                    cast("EQUIPMENT_TYPE_TXT" as character varying(40)) as "EQUIPMENT_TYPE_TXT" ,
                    cast("SPCL_INSTR_TXT" as character varying(1000)) as "SPCL_INSTR_TXT" ,
                    cast("SRVC_DURATION_MINS_NBR" as NUMBER(38,0)) as "SRVC_DURATION_MINS_NBR" ,
                    cast("REQURMNT_TXT" as character varying(40)) as "REQURMNT_TXT" ,
                    cast("PICKING_LIST_PRINTED_IND" as NUMBER(38,0)) as "PICKING_LIST_PRINTED_IND" ,
                    cast("PICKING_LIST_PRINTED_AFTER_CHG_IND" as NUMBER(38,0)) as "PICKING_LIST_PRINTED_AFTER_CHG_IND" ,
                    cast("DO_NOT_SEND_IND" as NUMBER(38,0)) as "DO_NOT_SEND_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_picking_list

            
        )

        
)

select * from v_opco_picking_list_lineage
  );
