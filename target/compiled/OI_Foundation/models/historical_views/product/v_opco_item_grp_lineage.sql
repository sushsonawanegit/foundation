with v_opco_item_grp_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ITEM_GRP_SK" as character varying(32)) as "OPCO_ITEM_GRP_SK" ,
                    cast("OPCO_ITEM_GRP_CK" as character varying(32)) as "OPCO_ITEM_GRP_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_ITEM_GRP_CD" as character varying(120)) as "SRC_ITEM_GRP_CD" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("SRC_ITEM_GRP_DESC" as character varying(240)) as "SRC_ITEM_GRP_DESC" ,
                    cast("PRODN_UOM_SK" as character varying(32)) as "PRODN_UOM_SK" ,
                    cast("SALES_UOM_SK" as character varying(32)) as "SALES_UOM_SK" ,
                    cast("OPCO_ITEM_FREIGHT_SK" as character varying(32)) as "OPCO_ITEM_FREIGHT_SK" ,
                    cast("CAPEX_ONLY_IND" as NUMBER(38,0)) as "CAPEX_ONLY_IND" ,
                    cast("CALC_CATEGORY_CD" as NUMBER(38,0)) as "CALC_CATEGORY_CD" ,
                    cast("ITEM_GRP_TYPE_TXT" as character varying(60)) as "ITEM_GRP_TYPE_TXT" ,
                    cast("EXPLICIT_FREIGHT_IND" as NUMBER(38,0)) as "EXPLICIT_FREIGHT_IND" ,
                    cast("EXPLICIT_FREIGHT_PCT" as NUMBER(28,12)) as "EXPLICIT_FREIGHT_PCT" ,
                    cast("FREIGHT_PCT" as NUMBER(28,12)) as "FREIGHT_PCT" ,
                    cast("IC_MARKUP_PCT" as NUMBER(28,12)) as "IC_MARKUP_PCT" ,
                    cast("IMBEDDED_FREIGHT_IND" as NUMBER(38,0)) as "IMBEDDED_FREIGHT_IND" ,
                    cast("IMBEDDED_FREIGHT_PCT" as NUMBER(28,12)) as "IMBEDDED_FREIGHT_PCT" ,
                    cast("MARKUP_PCT" as NUMBER(28,12)) as "MARKUP_PCT" ,
                    cast("PROJECT_ONLY_IND" as NUMBER(38,0)) as "PROJECT_ONLY_IND" ,
                    cast("PROJECT_CTGRY_CD" as character varying(80)) as "PROJECT_CTGRY_CD" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_grp

            
        )

        
)

select * from v_opco_item_grp_lineage