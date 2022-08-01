with v_opco_commsn_grp_lineage as(
    
    

        (
            select

                
                    cast("OPCO_COMMSN_GRP_SK" as character varying(32)) as "OPCO_COMMSN_GRP_SK" ,
                    cast("OPCO_COMMSN_GRP_CK" as character varying(32)) as "OPCO_COMMSN_GRP_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(15)) as "SRC_SYS_NM" ,
                    cast("SRC_COMMSN_GRP_CD" as character varying(50)) as "SRC_COMMSN_GRP_CD" ,
                    cast("SRC_COMMSN_GRP_DESC" as character varying(100)) as "SRC_COMMSN_GRP_DESC" ,
                    cast("SRC_COMMSN_GRP_TYPE_TXT" as character varying(20)) as "SRC_COMMSN_GRP_TYPE_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_commsn_grp

            
        )

        
)

select * from v_opco_commsn_grp_lineage