with v_opco_project_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_PROJECT_CK" as character varying(32)) as "OPCO_PROJECT_CK" ,
                    cast("OPCO_PROJECT_AK" as character varying(100)) as "OPCO_PROJECT_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("PROJECT_ID" as character varying(80)) as "PROJECT_ID" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("DLVRY_LOCN_SK" as character varying(32)) as "DLVRY_LOCN_SK" ,
                    cast("PROJECT_NM" as character varying(240)) as "PROJECT_NM" ,
                    cast("PROJECT_STATUS_TXT" as character varying(16777216)) as "PROJECT_STATUS_TXT" ,
                    cast("PROJECT_TYPE_TXT" as character varying(16777216)) as "PROJECT_TYPE_TXT" ,
                    cast("PROJECT_CRT_DT" as TIMESTAMP_NTZ) as "PROJECT_CRT_DT" ,
                    cast("PROJECT_START_DT" as TIMESTAMP_NTZ) as "PROJECT_START_DT" ,
                    cast("PROJECT_END_DT" as TIMESTAMP_NTZ) as "PROJECT_END_DT" ,
                    cast("ESTIMATED_START_DT" as TIMESTAMP_NTZ) as "ESTIMATED_START_DT" ,
                    cast("ESTIMATED_END_DT" as TIMESTAMP_NTZ) as "ESTIMATED_END_DT" ,
                    cast("ACTUAL_START_DT" as TIMESTAMP_NTZ) as "ACTUAL_START_DT" ,
                    cast("ACTUAL_END_DT" as TIMESTAMP_NTZ) as "ACTUAL_END_DT" ,
                    cast("ESTIMATOR_NM" as character varying(80)) as "ESTIMATOR_NM" ,
                    cast("PROJECT_MGR_NM" as character varying(80)) as "PROJECT_MGR_NM" ,
                    cast("GL_POSTING_IND" as NUMBER(38,0)) as "GL_POSTING_IND" ,
                    cast("SORT_1_ID" as character varying(40)) as "SORT_1_ID" ,
                    cast("SORT_2_ID" as character varying(40)) as "SORT_2_ID" ,
                    cast("SORT_3_ID" as character varying(40)) as "SORT_3_ID" ,
                    cast("SLS_GRP_TXT" as character varying(40)) as "SLS_GRP_TXT" ,
                    cast("TAX_GRP_ID" as character varying(44)) as "TAX_GRP_ID" ,
                    cast("LOCN_1_TXT" as character varying(60)) as "LOCN_1_TXT" ,
                    cast("LOCN_2_TXT" as character varying(60)) as "LOCN_2_TXT" ,
                    cast("RETENTION_TERMS_TXT" as character varying(60)) as "RETENTION_TERMS_TXT" ,
                    cast("RETENTION_DUE_DT" as TIMESTAMP_NTZ) as "RETENTION_DUE_DT" ,
                    cast("USE_TAX_IND" as NUMBER(38,0)) as "USE_TAX_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project

            
        )

        
)

select * from v_opco_project_lineage