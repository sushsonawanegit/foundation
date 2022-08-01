
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_trans_posting_lineage 
  
   as (
    with v_opco_project_trans_posting_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_TRANS_POSTING_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_POSTING_SK" ,
                    cast("OPCO_PROJECT_TRANS_POSTING_CK" as character varying(32)) as "OPCO_PROJECT_TRANS_POSTING_CK" ,
                    cast("OPCO_PROJECT_TRANS_POSTING_AK" as character varying(16777216)) as "OPCO_PROJECT_TRANS_POSTING_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("OPCO_PROJECT_TRANS_ORIGIN_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_ORIGIN_SK" ,
                    cast("OPCO_GL_TRANS_ORIGIN_SK" as character varying(32)) as "OPCO_GL_TRANS_ORIGIN_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_GL_ACCT_SK" as character varying(32)) as "OPCO_GL_ACCT_SK" ,
                    cast("PROJECT_TRANS_POSTING_ID" as character varying(80)) as "PROJECT_TRANS_POSTING_ID" ,
                    cast("GL_TRANS_DT" as TIMESTAMP_NTZ) as "GL_TRANS_DT" ,
                    cast("VOUCHER_NBR" as character varying(80)) as "VOUCHER_NBR" ,
                    cast("PROJECT_TRANS_DT" as TIMESTAMP_NTZ) as "PROJECT_TRANS_DT" ,
                    cast("PROJECT_TRANS_TYPE_TXT" as character varying(16777216)) as "PROJECT_TRANS_TYPE_TXT" ,
                    cast("PROJECT_TYPE_TXT" as character varying(16777216)) as "PROJECT_TYPE_TXT" ,
                    cast("PROJECT_COST_OR_SLS_TXT" as character varying(16777216)) as "PROJECT_COST_OR_SLS_TXT" ,
                    cast("PROJECT_ADJSTMT_REF_ID" as character varying(80)) as "PROJECT_ADJSTMT_REF_ID" ,
                    cast("PYMNT_DT" as TIMESTAMP_NTZ) as "PYMNT_DT" ,
                    cast("PYMNT_STATUS_TXT" as character varying(16777216)) as "PYMNT_STATUS_TXT" ,
                    cast("INVTRY_TRANS_ID" as character varying(80)) as "INVTRY_TRANS_ID" ,
                    cast("EMPL_ID" as character varying(16777216)) as "EMPL_ID" ,
                    cast("TRANS_QTY" as NUMBER(28,12)) as "TRANS_QTY" ,
                    cast("OPCO_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TRANS_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_trans_posting

            
        )

        
)

select * from v_opco_project_trans_posting_lineage
  );
