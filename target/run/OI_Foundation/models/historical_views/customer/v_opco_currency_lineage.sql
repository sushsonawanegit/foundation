
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_currency_lineage 
  
   as (
    with v_opco_currency as(
    
    

        (
            select

                
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("OPCO_CURRENCY_CK" as character varying(32)) as "OPCO_CURRENCY_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_CURRENCY_CD" as character varying(10)) as "SRC_CURRENCY_CD" ,
                    cast("SRC_CURRENCY_NM" as character varying(50)) as "SRC_CURRENCY_NM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_currency

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("OPCO_CURRENCY_CK" as character varying(32)) as "OPCO_CURRENCY_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_CURRENCY_CD" as character varying(10)) as "SRC_CURRENCY_CD" ,
                    cast("SRC_CURRENCY_NM" as character varying(50)) as "SRC_CURRENCY_NM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_currency

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("OPCO_CURRENCY_CK" as character varying(32)) as "OPCO_CURRENCY_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_CURRENCY_CD" as character varying(10)) as "SRC_CURRENCY_CD" ,
                    cast("SRC_CURRENCY_NM" as character varying(50)) as "SRC_CURRENCY_NM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_currency

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("OPCO_CURRENCY_CK" as character varying(32)) as "OPCO_CURRENCY_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_CURRENCY_CD" as character varying(10)) as "SRC_CURRENCY_CD" ,
                    cast("SRC_CURRENCY_NM" as character varying(50)) as "SRC_CURRENCY_NM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_currency

            
        )

        
)

select * from v_opco_currency
  );
