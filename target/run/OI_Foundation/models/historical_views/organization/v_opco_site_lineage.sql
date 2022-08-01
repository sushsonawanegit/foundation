
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_site_lineage 
  
   as (
    with v_opco_site_lineage as(
    
    

        (
            select

                
                    cast("OPCO_SITE_SK" as character varying(32)) as "OPCO_SITE_SK" ,
                    cast("OPCO_SITE_CK" as character varying(32)) as "OPCO_SITE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_SITE_ID" as character varying(120)) as "SRC_SITE_ID" ,
                    cast("SRC_SITE_NM" as character varying(240)) as "SRC_SITE_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_site

            
        )

        
)

select * from v_opco_site_lineage
  );
