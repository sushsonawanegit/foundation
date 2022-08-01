
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_locn_lineage 
  
   as (
    with v_opco_locn_lineage as(
    
    

        (
            select

                
                    cast("OPCO_LOCN_SK" as character varying(32)) as "OPCO_LOCN_SK" ,
                    cast("OPCO_LOCN_CK" as character varying(32)) as "OPCO_LOCN_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("STD_LOCN_SK" as character varying(1)) as "STD_LOCN_SK" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("ADDR_LN_1_TXT" as character varying(3000)) as "ADDR_LN_1_TXT" ,
                    cast("ADDR_LN_2_TXT" as character varying(1)) as "ADDR_LN_2_TXT" ,
                    cast("ADDR_LN_3_TXT" as character varying(1)) as "ADDR_LN_3_TXT" ,
                    cast("CITY_NM" as character varying(720)) as "CITY_NM" ,
                    cast("STATE_NM" as character varying(120)) as "STATE_NM" ,
                    cast("COUNTRY_NM" as character varying(120)) as "COUNTRY_NM" ,
                    cast("ZIP_CD" as character varying(120)) as "ZIP_CD" ,
                    cast("COUNTY_NM" as character varying(360)) as "COUNTY_NM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn

            
        )

        
)

select * from v_opco_locn_lineage
  );
