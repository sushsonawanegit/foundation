with v_opco_pymnt_mode_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("OPCO_PYMNT_MODE_CK" as character varying(32)) as "OPCO_PYMNT_MODE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_PYMNT_MODE_CD" as character varying(10)) as "SRC_PYMNT_MODE_CD" ,
                    cast("SRC_PYMNT_MODE_DESC" as character varying(100)) as "SRC_PYMNT_MODE_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_pymnt_mode

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("OPCO_PYMNT_MODE_CK" as character varying(32)) as "OPCO_PYMNT_MODE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_PYMNT_MODE_CD" as character varying(10)) as "SRC_PYMNT_MODE_CD" ,
                    cast("SRC_PYMNT_MODE_DESC" as character varying(100)) as "SRC_PYMNT_MODE_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_pymnt_mode

            
        )

        
)

select * from v_opco_pymnt_mode_lineage