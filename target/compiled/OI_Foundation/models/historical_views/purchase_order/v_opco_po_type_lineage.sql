with v_opco_po_type_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PO_TYPE_SK" as character varying(32)) as "OPCO_PO_TYPE_SK" ,
                    cast("OPCO_PO_TYPE_CK" as character varying(32)) as "OPCO_PO_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_PO_TYPE_CD" as NUMBER(1,0)) as "SRC_PO_TYPE_CD" ,
                    cast("SRC_PO_TYPE_DESC" as character varying(14)) as "SRC_PO_TYPE_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_type

            
        )

        
)

select * from v_opco_po_type_lineage