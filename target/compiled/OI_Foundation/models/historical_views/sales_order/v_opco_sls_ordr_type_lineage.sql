with v_opco_sls_ordr_type_lineage as(
    
    

        (
            select

                
                    cast("OPCO_SLS_ORDR_TYPE_SK" as character varying(32)) as "OPCO_SLS_ORDR_TYPE_SK" ,
                    cast("OPCO_SLS_ORDR_TYPE_CK" as character varying(32)) as "OPCO_SLS_ORDR_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_SLS_ORDR_TYPE_CD" as NUMBER(1,0)) as "SRC_SLS_ORDR_TYPE_CD" ,
                    cast("SRC_SLS_ORDR_DOC_STATUS_DESC" as character varying(17)) as "SRC_SLS_ORDR_DOC_STATUS_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_type

            
        )

        
)

select * from v_opco_sls_ordr_type_lineage