with v_opco_uom as(
    
    

        (
            select

                
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_UOM_CK" as character varying(32)) as "OPCO_UOM_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_UOM_CD" as character varying(16777216)) as "SRC_UOM_CD" ,
                    cast("SRC_UOM_DESC" as character varying(16777216)) as "SRC_UOM_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_uom

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_UOM_CK" as character varying(32)) as "OPCO_UOM_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_UOM_CD" as character varying(16777216)) as "SRC_UOM_CD" ,
                    cast("SRC_UOM_DESC" as character varying(16777216)) as "SRC_UOM_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_uom

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_UOM_CK" as character varying(32)) as "OPCO_UOM_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_UOM_CD" as character varying(16777216)) as "SRC_UOM_CD" ,
                    cast("SRC_UOM_DESC" as character varying(16777216)) as "SRC_UOM_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_uom

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_UOM_CK" as character varying(32)) as "OPCO_UOM_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(16777216)) as "SRC_SYS_NM" ,
                    cast("SRC_UOM_CD" as character varying(16777216)) as "SRC_UOM_CD" ,
                    cast("SRC_UOM_DESC" as character varying(16777216)) as "SRC_UOM_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_uom

            
        )

        
)

select * from v_opco_uom