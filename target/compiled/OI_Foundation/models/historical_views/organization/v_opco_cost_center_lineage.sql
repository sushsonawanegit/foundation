with v_opco_cost_center as(
    
    

        (
            select

                
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_COST_CENTER_CK" as character varying(32)) as "OPCO_COST_CENTER_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_COST_CENTER_CD" as character varying(30)) as "SRC_COST_CENTER_CD" ,
                    cast("SRC_COST_CENTER_DESC" as character varying(100)) as "SRC_COST_CENTER_DESC" ,
                    cast("COST_CENTER_WO_SPCL_CHR_CD" as character varying(16777216)) as "COST_CENTER_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("SRC_COST_CENTER_TYPE_TXT" as character varying(16777216)) as "SRC_COST_CENTER_TYPE_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cost_center

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_COST_CENTER_CK" as character varying(32)) as "OPCO_COST_CENTER_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_COST_CENTER_CD" as character varying(30)) as "SRC_COST_CENTER_CD" ,
                    cast("SRC_COST_CENTER_DESC" as character varying(100)) as "SRC_COST_CENTER_DESC" ,
                    cast("COST_CENTER_WO_SPCL_CHR_CD" as character varying(16777216)) as "COST_CENTER_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("SRC_COST_CENTER_TYPE_TXT" as character varying(16777216)) as "SRC_COST_CENTER_TYPE_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_cost_center

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_COST_CENTER_CK" as character varying(32)) as "OPCO_COST_CENTER_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_COST_CENTER_CD" as character varying(30)) as "SRC_COST_CENTER_CD" ,
                    cast("SRC_COST_CENTER_DESC" as character varying(100)) as "SRC_COST_CENTER_DESC" ,
                    cast("COST_CENTER_WO_SPCL_CHR_CD" as character varying(16777216)) as "COST_CENTER_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("SRC_COST_CENTER_TYPE_TXT" as character varying(16777216)) as "SRC_COST_CENTER_TYPE_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_cost_center

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_COST_CENTER_CK" as character varying(32)) as "OPCO_COST_CENTER_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_COST_CENTER_CD" as character varying(30)) as "SRC_COST_CENTER_CD" ,
                    cast("SRC_COST_CENTER_DESC" as character varying(100)) as "SRC_COST_CENTER_DESC" ,
                    cast("COST_CENTER_WO_SPCL_CHR_CD" as character varying(16777216)) as "COST_CENTER_WO_SPCL_CHR_CD" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("SRC_COST_CENTER_TYPE_TXT" as character varying(16777216)) as "SRC_COST_CENTER_TYPE_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_cost_center

            
        )

        
)

select * from v_opco_cost_center