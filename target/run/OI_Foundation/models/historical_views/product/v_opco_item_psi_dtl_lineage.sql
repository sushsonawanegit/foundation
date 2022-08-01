
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_item_psi_dtl_lineage 
  
   as (
    with v_opco_item_psi_dtl_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ITEM_PSI_DTL_SK" as character varying(32)) as "OPCO_ITEM_PSI_DTL_SK" ,
                    cast("OPCO_ITEM_PSI_DTL_CK" as character varying(32)) as "OPCO_ITEM_PSI_DTL_CK" ,
                    cast("OPCO_ITEM_PSI_DTL_AK" as character varying(16777216)) as "OPCO_ITEM_PSI_DTL_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_ITEM_CD" as character varying(80)) as "SRC_ITEM_CD" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("PSI_TYPE_TXT" as character varying(16777216)) as "PSI_TYPE_TXT" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("PSI_PRICE_DT" as TIMESTAMP_NTZ) as "PSI_PRICE_DT" ,
                    cast("PSI_PRICE_AMT" as NUMBER(28,12)) as "PSI_PRICE_AMT" ,
                    cast("PSI_PRICE_UNIT_QTY" as NUMBER(28,12)) as "PSI_PRICE_UNIT_QTY" ,
                    cast("PSI_DLVRY_QTY" as NUMBER(28,12)) as "PSI_DLVRY_QTY" ,
                    cast("PSI_LINE_DSCNT_AMT" as character varying(40)) as "PSI_LINE_DSCNT_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_psi_dtl

            
        )

        
)

select * from v_opco_item_psi_dtl_lineage
  );
