
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_warehouse_lineage 
  
   as (
    with v_warehouse_lineage as(
    
    

        (
            select

                
                    cast("WAREHOUSE_SK" as character varying(32)) as "WAREHOUSE_SK" ,
                    cast("WAREHOUSE_CK" as character varying(32)) as "WAREHOUSE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("WAREHOUSE_ID" as character varying(120)) as "WAREHOUSE_ID" ,
                    cast("WAREHOUSE_NM" as character varying(240)) as "WAREHOUSE_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse

            
        )

        
)

select * from v_warehouse_lineage
  );
