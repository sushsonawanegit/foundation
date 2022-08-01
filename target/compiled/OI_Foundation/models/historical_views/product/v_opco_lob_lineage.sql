with v_opco_lob as(
    
    

        (
            select

                
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_LOB_CK" as character varying(32)) as "OPCO_LOB_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(11)) as "SRC_SYS_NM" ,
                    cast("SRC_LOB_CD" as character varying(90)) as "SRC_LOB_CD" ,
                    cast("SRC_LOB_NM" as character varying(360)) as "SRC_LOB_NM" ,
                    cast("LOB_WO_SPCL_CHR_CD" as character varying(16777216)) as "LOB_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_LOB_CK" as character varying(32)) as "OPCO_LOB_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(11)) as "SRC_SYS_NM" ,
                    cast("SRC_LOB_CD" as character varying(90)) as "SRC_LOB_CD" ,
                    cast("SRC_LOB_NM" as character varying(360)) as "SRC_LOB_NM" ,
                    cast("LOB_WO_SPCL_CHR_CD" as character varying(16777216)) as "LOB_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_lob

            
        )

        
)

select * from v_opco_lob