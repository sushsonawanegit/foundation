with v_opco_opco_locn_xref_lineage as(
    
    

        (
            select

                
                    cast("OPCO_OPCO_LOCN_XREF_CK" as character varying(32)) as "OPCO_OPCO_LOCN_XREF_CK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_LOCN_SK" as character varying(32)) as "OPCO_LOCN_SK" ,
                    cast("OPCO_OPCO_LOCN_ASSCN_TYPE_TXT" as character varying(4)) as "OPCO_OPCO_LOCN_ASSCN_TYPE_TXT" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("CONTACT_TYPE_TXT" as character varying(1)) as "CONTACT_TYPE_TXT" ,
                    cast("CONTACT_NM" as character varying(1)) as "CONTACT_NM" ,
                    cast("CONTACT_PH_NBR" as character varying(40)) as "CONTACT_PH_NBR" ,
                    cast("CONTACT_PH_EXTN_NBR" as character varying(1)) as "CONTACT_PH_EXTN_NBR" ,
                    cast("CONTACT_CELL_NBR" as character varying(40)) as "CONTACT_CELL_NBR" ,
                    cast("CONTACT_LOCAL_PH_NBR" as character varying(20)) as "CONTACT_LOCAL_PH_NBR" ,
                    cast("CONTACT_FAX_NBR" as character varying(40)) as "CONTACT_FAX_NBR" ,
                    cast("CONTACT_TELEX_NBR" as character varying(1)) as "CONTACT_TELEX_NBR" ,
                    cast("CONTACT_EMAIL_ID" as character varying(160)) as "CONTACT_EMAIL_ID" ,
                    cast("LANGUAGE_TXT" as character varying(42)) as "LANGUAGE_TXT" ,
                    cast("START_DT" as character varying(1)) as "START_DT" ,
                    cast("END_DT" as character varying(1)) as "END_DT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_opco_locn_xref

            
        )

        
)

select * from v_opco_opco_locn_xref_lineage