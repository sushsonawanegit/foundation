
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_dept_lineage 
  
   as (
    with v_opco_dept as(
    
    

        (
            select

                
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_DEPT_CK" as character varying(32)) as "OPCO_DEPT_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_DEPT_CD" as character varying(135)) as "SRC_DEPT_CD" ,
                    cast("SRC_DEPT_DESC" as character varying(100)) as "SRC_DEPT_DESC" ,
                    cast("DEPT_WO_SPCL_CHR_CD" as character varying(16777216)) as "DEPT_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_DEPT_CK" as character varying(32)) as "OPCO_DEPT_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_DEPT_CD" as character varying(135)) as "SRC_DEPT_CD" ,
                    cast("SRC_DEPT_DESC" as character varying(100)) as "SRC_DEPT_DESC" ,
                    cast("DEPT_WO_SPCL_CHR_CD" as character varying(16777216)) as "DEPT_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_dept

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_DEPT_CK" as character varying(32)) as "OPCO_DEPT_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_DEPT_CD" as character varying(135)) as "SRC_DEPT_CD" ,
                    cast("SRC_DEPT_DESC" as character varying(100)) as "SRC_DEPT_DESC" ,
                    cast("DEPT_WO_SPCL_CHR_CD" as character varying(16777216)) as "DEPT_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_dept

            
        )

        
)

select * from v_opco_dept
  );
