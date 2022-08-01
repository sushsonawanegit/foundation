
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_item_opco_assctn_xref_lineage 
  
   as (
    with v_opco_item_opco_assctn_xref_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_ITEM_OPCO_ASSCTN_XREF_CK" as character varying(32)) as "OPCO_ITEM_OPCO_ASSCTN_XREF_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("OPCO_ITEM_COUNT_GRP_SK" as character varying(1)) as "OPCO_ITEM_COUNT_GRP_SK" ,
                    cast("OPCO_ITEM_SCRAP_TYPE_SK" as character varying(1)) as "OPCO_ITEM_SCRAP_TYPE_SK" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("BIN_LOCN_TXT" as character varying(40)) as "BIN_LOCN_TXT" ,
                    cast("BLDG_LOCN_TXT" as character varying(40)) as "BLDG_LOCN_TXT" ,
                    cast("CUBIC_YRDS_AMT" as NUMBER(28,12)) as "CUBIC_YRDS_AMT" ,
                    cast("CUBIC_YRDS_FIXED_AMT" as NUMBER(28,12)) as "CUBIC_YRDS_FIXED_AMT" ,
                    cast("CUBIC_YRDS_VARIABLE_AMT" as NUMBER(28,12)) as "CUBIC_YRDS_VARIABLE_AMT" ,
                    cast("FIXED_WT_AMT" as NUMBER(28,12)) as "FIXED_WT_AMT" ,
                    cast("GL_PRODN_QTY" as NUMBER(28,12)) as "GL_PRODN_QTY" ,
                    cast("GL_SLS_QTY" as NUMBER(28,12)) as "GL_SLS_QTY" ,
                    cast("LABOR_HRS_NBR" as NUMBER(28,12)) as "LABOR_HRS_NBR" ,
                    cast("MAX_ON_HAND_QTY" as NUMBER(28,12)) as "MAX_ON_HAND_QTY" ,
                    cast("NET_WT_AMT" as NUMBER(28,12)) as "NET_WT_AMT" ,
                    cast("PRODN_ABCD_CD" as NUMBER(38,0)) as "PRODN_ABCD_CD" ,
                    cast("PRODN_FLUSHING_PRINCIPLE_CD" as NUMBER(38,0)) as "PRODN_FLUSHING_PRINCIPLE_CD" ,
                    cast("VARIABLE_WT_AMT" as NUMBER(28,12)) as "VARIABLE_WT_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_opco_assctn_xref

            
        )

        
)

select * from v_opco_item_opco_assctn_xref_lineage
  );
