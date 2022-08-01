
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_catgry_lineage 
  
   as (
    with v_opco_project_catgry_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("OPCO_PROJECT_CATGRY_CK" as character varying(32)) as "OPCO_PROJECT_CATGRY_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("CATGRY_CD" as character varying(80)) as "CATGRY_CD" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("CATGRY_NM" as character varying(240)) as "CATGRY_NM" ,
                    cast("CATGRY_GRP_CD" as character varying(40)) as "CATGRY_GRP_CD" ,
                    cast("CATGRY_TYPE_TXT" as character varying(16777216)) as "CATGRY_TYPE_TXT" ,
                    cast("EMPL_APPLICABLE_IND" as character varying(16777216)) as "EMPL_APPLICABLE_IND" ,
                    cast("ACTV_IND" as NUMBER(38,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_catgry

            
        )

        
)

select * from v_opco_project_catgry_lineage
  );
