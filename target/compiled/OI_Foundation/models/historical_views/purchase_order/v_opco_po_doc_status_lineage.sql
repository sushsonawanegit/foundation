with v_opco_po_doc_status_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PO_DOC_STATUS_SK" as character varying(32)) as "OPCO_PO_DOC_STATUS_SK" ,
                    cast("OPCO_PO_DOC_STATUS_CK" as character varying(32)) as "OPCO_PO_DOC_STATUS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("PO_DOC_STATUS_CD" as NUMBER(2,0)) as "PO_DOC_STATUS_CD" ,
                    cast("PO_DOC_STATUS_DESC" as character varying(26)) as "PO_DOC_STATUS_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_doc_status

            
        )

        
)

select * from v_opco_po_doc_status_lineage