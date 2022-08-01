with v_opco_item_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_ITEM_CK" as character varying(32)) as "OPCO_ITEM_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_ITEM_CD" as character varying(80)) as "SRC_ITEM_CD" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("SRC_ITEM_NM" as character varying(240)) as "SRC_ITEM_NM" ,
                    cast("OPCO_ITEM_TYPE_SK" as character varying(32)) as "OPCO_ITEM_TYPE_SK" ,
                    cast("OPCO_ITEM_SUBTYPE_SK" as character varying(32)) as "OPCO_ITEM_SUBTYPE_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_COMMSN_GRP_SK" as character varying(32)) as "OPCO_COMMSN_GRP_SK" ,
                    cast("OPCO_ITEM_DIM_GRP_SK" as character varying(1)) as "OPCO_ITEM_DIM_GRP_SK" ,
                    cast("OPCO_ITEM_BUYER_GRP_SK" as character varying(1)) as "OPCO_ITEM_BUYER_GRP_SK" ,
                    cast("OPCO_ITEM_GRP_SK" as character varying(32)) as "OPCO_ITEM_GRP_SK" ,
                    cast("OPCO_ITEM_MODEL_GRP_SK" as character varying(32)) as "OPCO_ITEM_MODEL_GRP_SK" ,
                    cast("PRIMARY_VENDOR_SK" as character varying(32)) as "PRIMARY_VENDOR_SK" ,
                    cast("OPCO_ITEM_CLASS_SK" as character varying(32)) as "OPCO_ITEM_CLASS_SK" ,
                    cast("OPCO_ITEM_CVRG_GRP_SK" as character varying(32)) as "OPCO_ITEM_CVRG_GRP_SK" ,
                    cast("OPCO_ITEM_SLS_CLASS_SK" as character varying(32)) as "OPCO_ITEM_SLS_CLASS_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("HEIGHT_NBR" as NUMBER(28,12)) as "HEIGHT_NBR" ,
                    cast("WIDTH_NBR" as NUMBER(28,12)) as "WIDTH_NBR" ,
                    cast("DEPTH_NBR" as NUMBER(28,12)) as "DEPTH_NBR" ,
                    cast("NET_WT_NBR" as NUMBER(28,12)) as "NET_WT_NBR" ,
                    cast("COST_MODEL_IND" as NUMBER(38,0)) as "COST_MODEL_IND" ,
                    cast("COST_GROUP_TXT" as character varying(40)) as "COST_GROUP_TXT" ,
                    cast("SRC_CRT_DTM" as TIMESTAMP_NTZ) as "SRC_CRT_DTM" ,
                    cast("SRC_CRT_BY" as character varying(20)) as "SRC_CRT_BY" ,
                    cast("SRC_UPDT_DTM" as TIMESTAMP_NTZ) as "SRC_UPDT_DTM" ,
                    cast("SRC_UPDT_BY" as character varying(20)) as "SRC_UPDT_BY" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("EXPLICIT_FREIGHT_IND" as NUMBER(38,0)) as "EXPLICIT_FREIGHT_IND" ,
                    cast("FORM_ID_TXT" as character varying(80)) as "FORM_ID_TXT" ,
                    cast("FORM_GRP_TXT" as character varying(80)) as "FORM_GRP_TXT" ,
                    cast("ITEM_MOLD_TXT" as character varying(60)) as "ITEM_MOLD_TXT" ,
                    cast("ITEM_ADMNSTRN_TYPE_DESC" as character varying(16777216)) as "ITEM_ADMNSTRN_TYPE_DESC" ,
                    cast("KITS_IND" as NUMBER(38,0)) as "KITS_IND" ,
                    cast("ITEM_CATGRY_TXT" as character varying(80)) as "ITEM_CATGRY_TXT" ,
                    cast("PHANTOM_ITEM_IND" as NUMBER(38,0)) as "PHANTOM_ITEM_IND" ,
                    cast("PURCHASE_PRICE_UPDT_IND" as NUMBER(38,0)) as "PURCHASE_PRICE_UPDT_IND" ,
                    cast("SHIPPING_CLASS_TXT" as character varying(120)) as "SHIPPING_CLASS_TXT" ,
                    cast("SKU_FAMILY_TXT" as character varying(80)) as "SKU_FAMILY_TXT" ,
                    cast("ITEM_SUBCATGRY_TXT" as character varying(80)) as "ITEM_SUBCATGRY_TXT" ,
                    cast("CARTON_UPC_TXT" as character varying(60)) as "CARTON_UPC_TXT" ,
                    cast("ITEM_UPC_TXT" as character varying(60)) as "ITEM_UPC_TXT" ,
                    cast("VITALITY_DT" as TIMESTAMP_NTZ) as "VITALITY_DT" ,
                    cast("TIER_TXT" as character varying(16777216)) as "TIER_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item

            
        )

        
)

select * from v_opco_item_lineage