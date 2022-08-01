with v_opco_sls_ordr_doc_status_lineage as(
    
    

        (
            select

                
                    cast("OPCO_SLS_ORDR_DOC_STATUS_SK" as character varying(32)) as "OPCO_SLS_ORDR_DOC_STATUS_SK" ,
                    cast("OPCO_SLS_ORDR_DOC_STATUS_CK" as character varying(32)) as "OPCO_SLS_ORDR_DOC_STATUS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_SLS_ORDR_DOC_STATUS_CD" as NUMBER(2,0)) as "SRC_SLS_ORDR_DOC_STATUS_CD" ,
                    cast("SRC_SLS_ORDR_DOC_STATUS_DESC" as character varying(26)) as "SRC_SLS_ORDR_DOC_STATUS_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_doc_status

            
        )

        
)

select * from v_opco_sls_ordr_doc_status_lineage