with v_opco_vendor_packing_slip_lineage as(
    
    

        (
            select

                
                    cast("OPCO_VENDOR_PACKING_SLIP_SK" as character varying(32)) as "OPCO_VENDOR_PACKING_SLIP_SK" ,
                    cast("OPCO_VENDOR_PACKING_SLIP_CK" as character varying(32)) as "OPCO_VENDOR_PACKING_SLIP_CK" ,
                    cast("OPCO_VENDOR_PACKING_SLIP_AK" as character varying(93)) as "OPCO_VENDOR_PACKING_SLIP_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("PACKING_SLIP_ID" as character varying(40)) as "PACKING_SLIP_ID" ,
                    cast("INTERNAL_PACKING_SLIP_ID" as character varying(40)) as "INTERNAL_PACKING_SLIP_ID" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_VENDOR_SK" as character varying(32)) as "OPCO_VENDOR_SK" ,
                    cast("OPCO_INVOICE_VENDOR_SK" as character varying(32)) as "OPCO_INVOICE_VENDOR_SK" ,
                    cast("OPCO_PO_SK" as character varying(32)) as "OPCO_PO_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("DLVRY_LOCN_SK" as character varying(32)) as "DLVRY_LOCN_SK" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_PO_TYPE_SK" as character varying(32)) as "OPCO_PO_TYPE_SK" ,
                    cast("DLVRY_DT" as TIMESTAMP_NTZ) as "DLVRY_DT" ,
                    cast("DLVRY_VENDOR_NM" as character varying(120)) as "DLVRY_VENDOR_NM" ,
                    cast("GL_VOUCHER_NBR" as character varying(40)) as "GL_VOUCHER_NBR" ,
                    cast("ITEM_BUYER_GRP_ID" as character varying(20)) as "ITEM_BUYER_GRP_ID" ,
                    cast("PURCHASER_NM" as character varying(40)) as "PURCHASER_NM" ,
                    cast("RQSTD_BY_NM" as character varying(120)) as "RQSTD_BY_NM" ,
                    cast("REQUSITIONER_NM" as character varying(40)) as "REQUSITIONER_NM" ,
                    cast("ATTN_INFO_TXT" as character varying(510)) as "ATTN_INFO_TXT" ,
                    cast("DLVRY_QTY" as NUMBER(28,12)) as "DLVRY_QTY" ,
                    cast("DLVRY_VOLUME_AMT" as NUMBER(28,12)) as "DLVRY_VOLUME_AMT" ,
                    cast("DLVRY_WEIGHT_AMT" as NUMBER(28,12)) as "DLVRY_WEIGHT_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip

            
        )

        
)

select * from v_opco_vendor_packing_slip_lineage