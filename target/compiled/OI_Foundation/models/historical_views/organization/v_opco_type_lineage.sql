with v_opco_type as(
    
    

        (
            select

                
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_TYPE_CK" as character varying(32)) as "OPCO_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_TYPE_CD" as character varying(135)) as "SRC_TYPE_CD" ,
                    cast("SRC_TYPE_DESC" as character varying(100)) as "SRC_TYPE_DESC" ,
                    cast("TYPE_WO_SPCL_CHR_CD" as character varying(16777216)) as "TYPE_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_TYPE_CK" as character varying(32)) as "OPCO_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_TYPE_CD" as character varying(135)) as "SRC_TYPE_CD" ,
                    cast("SRC_TYPE_DESC" as character varying(100)) as "SRC_TYPE_DESC" ,
                    cast("TYPE_WO_SPCL_CHR_CD" as character varying(16777216)) as "TYPE_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_type

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_TYPE_CK" as character varying(32)) as "OPCO_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_TYPE_CD" as character varying(135)) as "SRC_TYPE_CD" ,
                    cast("SRC_TYPE_DESC" as character varying(100)) as "SRC_TYPE_DESC" ,
                    cast("TYPE_WO_SPCL_CHR_CD" as character varying(16777216)) as "TYPE_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_type

            
        )

        
)

select * from v_opco_type