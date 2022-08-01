with v_opco_dlvry_terms_lineage as(
    
    

        (
            select

                
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_DLVRY_TERMS_CK" as character varying(32)) as "OPCO_DLVRY_TERMS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_DLVRY_TERMS_CD" as character varying(60)) as "SRC_DLVRY_TERMS_CD" ,
                    cast("SRC_DLVRY_TERMS_DESC" as character varying(120)) as "SRC_DLVRY_TERMS_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_terms

            
        )

        
)

select * from v_opco_dlvry_terms_lineage