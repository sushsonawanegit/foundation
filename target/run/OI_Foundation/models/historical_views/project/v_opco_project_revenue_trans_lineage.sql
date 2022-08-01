
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_revenue_trans_lineage 
  
   as (
    with v_opco_project_revenue_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_REVENUE_TRANS_SK" as character varying(32)) as "OPCO_PROJECT_REVENUE_TRANS_SK" ,
                    cast("OPCO_PROJECT_REVENUE_TRANS_CK" as character varying(32)) as "OPCO_PROJECT_REVENUE_TRANS_CK" ,
                    cast("OPCO_PROJECT_REVENUE_TRANS_AK" as character varying(16777216)) as "OPCO_PROJECT_REVENUE_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("CHG_NBR" as character varying(40)) as "CHG_NBR" ,
                    cast("PROJECT_REVENUE_TRANS_TYPE_CD" as character varying(40)) as "PROJECT_REVENUE_TRANS_TYPE_CD" ,
                    cast("PROJECT_REVENUE_TRANS_DESC" as character varying(120)) as "PROJECT_REVENUE_TRANS_DESC" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("APPRVD_IND" as NUMBER(38,0)) as "APPRVD_IND" ,
                    cast("PROJECT_REVENUE_TRANS_AMT" as NUMBER(28,12)) as "PROJECT_REVENUE_TRANS_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_revenue_trans

            
        )

        
)

select * from v_opco_project_revenue_trans_lineage
  );
