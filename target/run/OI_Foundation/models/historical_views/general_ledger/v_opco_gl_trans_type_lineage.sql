
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_gl_trans_type_lineage 
  
   as (
    with v_opco_gl_trans_type as(
    
    

        (
            select

                
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_CK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_GL_TRANS_TYPE_CD" as character varying(16777216)) as "SRC_GL_TRANS_TYPE_CD" ,
                    cast("SRC_GL_TRANS_TYPE_DESC" as character varying(100)) as "SRC_GL_TRANS_TYPE_DESC" ,
                    cast(null as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast(null as TIMESTAMP_TZ) as "DELETE_DTM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans_type

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_CK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_GL_TRANS_TYPE_CD" as character varying(16777216)) as "SRC_GL_TRANS_TYPE_CD" ,
                    cast("SRC_GL_TRANS_TYPE_DESC" as character varying(100)) as "SRC_GL_TRANS_TYPE_DESC" ,
                    cast(null as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast(null as TIMESTAMP_TZ) as "DELETE_DTM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_gl_trans_type

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_CK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_GL_TRANS_TYPE_CD" as character varying(16777216)) as "SRC_GL_TRANS_TYPE_CD" ,
                    cast("SRC_GL_TRANS_TYPE_DESC" as character varying(100)) as "SRC_GL_TRANS_TYPE_DESC" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans_type

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_CK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_GL_TRANS_TYPE_CD" as character varying(16777216)) as "SRC_GL_TRANS_TYPE_CD" ,
                    cast("SRC_GL_TRANS_TYPE_DESC" as character varying(100)) as "SRC_GL_TRANS_TYPE_DESC" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans_type

            
        )

        
)

select * from v_opco_gl_trans_type
  );
