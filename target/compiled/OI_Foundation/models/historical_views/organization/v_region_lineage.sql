with v_region as(
    
    

        (
            select

                
                    cast("REGION_SK" as character varying(32)) as "REGION_SK" ,
                    cast("REGION_CK" as character varying(32)) as "REGION_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("REGION_ID" as character varying(25)) as "REGION_ID" ,
                    cast("REGION_NM" as character varying(50)) as "REGION_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_region

            
        )

        union all
        

        (
            select

                
                    cast("REGION_SK" as character varying(32)) as "REGION_SK" ,
                    cast("REGION_CK" as character varying(32)) as "REGION_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("REGION_ID" as character varying(25)) as "REGION_ID" ,
                    cast("REGION_NM" as character varying(50)) as "REGION_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_region

            
        )

        
)

select * from v_region