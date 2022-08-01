with v_sls_ordr_opco_locn_xref_lineage as(
    
    

        (
            select

                
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("OPCO_LOCN_SK" as character varying(32)) as "OPCO_LOCN_SK" ,
                    cast("SLS_ORDR_OPCO_LOCN_ASSCN_TYPE_TXT" as character varying(8)) as "SLS_ORDR_OPCO_LOCN_ASSCN_TYPE_TXT" ,
                    cast("SLS_ORDR_OPCO_LOCN_XREF_CK" as character varying(32)) as "SLS_ORDR_OPCO_LOCN_XREF_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("CONTACT_TYPE_TXT" as character varying(1)) as "CONTACT_TYPE_TXT" ,
                    cast("CONTACT_NM" as character varying(120)) as "CONTACT_NM" ,
                    cast("CONTACT_PH_NBR" as character varying(40)) as "CONTACT_PH_NBR" ,
                    cast("CONTACT_PH_EXTN_NBR" as character varying(1)) as "CONTACT_PH_EXTN_NBR" ,
                    cast("CONTACT_CELL_NBR" as character varying(40)) as "CONTACT_CELL_NBR" ,
                    cast("CONTACT_LOCAL_PH_NBR" as character varying(20)) as "CONTACT_LOCAL_PH_NBR" ,
                    cast("CONTACT_FAX_NBR" as character varying(40)) as "CONTACT_FAX_NBR" ,
                    cast("CONTACT_TELEX_NBR" as character varying(40)) as "CONTACT_TELEX_NBR" ,
                    cast("CONTACT_EMAIL_ID" as character varying(160)) as "CONTACT_EMAIL_ID" ,
                    cast("LANGUATE_TXT" as character varying(42)) as "LANGUATE_TXT" ,
                    cast("START_DT" as character varying(1)) as "START_DT" ,
                    cast("END_DT" as character varying(1)) as "END_DT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_sls_ordr_opco_locn_xref

            
        )

        
)

select * from v_sls_ordr_opco_locn_xref_lineage