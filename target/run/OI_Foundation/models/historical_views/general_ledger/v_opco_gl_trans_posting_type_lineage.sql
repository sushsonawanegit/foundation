
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_gl_trans_posting_type_lineage 
  
   as (
    with v_opco_gl_trans_posting_type as(
    
    

        (
            select

                
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_CK" as character varying(32)) as "OPCO_GL_TRANS_POSTING_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_GL_TRANS_POSTING_TYPE_CD" as NUMBER(3,0)) as "SRC_GL_TRANS_POSTING_TYPE_CD" ,
                    cast("SRC_GL_TRANS_POSTING_TYPE_DESC" as character varying(51)) as "SRC_GL_TRANS_POSTING_TYPE_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans_posting_type

            
        )

        
)

select * from v_opco_gl_trans_posting_type
  );
