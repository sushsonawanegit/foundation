
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_purpose_lineage 
  
   as (
    with v_opco_purpose as(
    
    

        (
            select

                
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_PURPOSE_CK" as character varying(32)) as "OPCO_PURPOSE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_PURPOSE_CD" as character varying(135)) as "SRC_PURPOSE_CD" ,
                    cast("SRC_PURPOSE_DESC" as character varying(100)) as "SRC_PURPOSE_DESC" ,
                    cast("PURPOSE_WO_SPCL_CHR_CD" as character varying(16777216)) as "PURPOSE_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_purpose

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_PURPOSE_CK" as character varying(32)) as "OPCO_PURPOSE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_PURPOSE_CD" as character varying(135)) as "SRC_PURPOSE_CD" ,
                    cast("SRC_PURPOSE_DESC" as character varying(100)) as "SRC_PURPOSE_DESC" ,
                    cast("PURPOSE_WO_SPCL_CHR_CD" as character varying(16777216)) as "PURPOSE_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_purpose

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_PURPOSE_CK" as character varying(32)) as "OPCO_PURPOSE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_PURPOSE_CD" as character varying(135)) as "SRC_PURPOSE_CD" ,
                    cast("SRC_PURPOSE_DESC" as character varying(100)) as "SRC_PURPOSE_DESC" ,
                    cast("PURPOSE_WO_SPCL_CHR_CD" as character varying(16777216)) as "PURPOSE_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_purpose

            
        )

        
)

select * from v_opco_purpose
  );
