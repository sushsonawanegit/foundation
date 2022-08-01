
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_sub_region_lineage 
  
   as (
    with v_sub_region as(
    
    

        (
            select

                
                    cast("SUB_REGION_SK" as character varying(32)) as "SUB_REGION_SK" ,
                    cast("SUB_REGION_CK" as character varying(32)) as "SUB_REGION_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SUB_REGION_ID" as character varying(20)) as "SUB_REGION_ID" ,
                    cast("SUB_REGION_NM" as character varying(100)) as "SUB_REGION_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_sub_region

            
        )

        union all
        

        (
            select

                
                    cast("SUB_REGION_SK" as character varying(32)) as "SUB_REGION_SK" ,
                    cast("SUB_REGION_CK" as character varying(32)) as "SUB_REGION_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SUB_REGION_ID" as character varying(20)) as "SUB_REGION_ID" ,
                    cast("SUB_REGION_NM" as character varying(100)) as "SUB_REGION_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_sub_region

            
        )

        
)

select * from v_sub_region
  );
