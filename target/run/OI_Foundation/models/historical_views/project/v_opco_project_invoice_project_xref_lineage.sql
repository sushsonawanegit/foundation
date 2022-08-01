
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_invoice_project_xref_lineage 
  
   as (
    with v_opco_project_invoice_project_xr_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("INVOICE_PROJECT_SK" as character varying(32)) as "INVOICE_PROJECT_SK" ,
                    cast("OPCO_PROJECT_INVOICE_PROJECT_XREF_CK" as character varying(32)) as "OPCO_PROJECT_INVOICE_PROJECT_XREF_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("INVOICE_PROJECT_ID" as character varying(40)) as "INVOICE_PROJECT_ID" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_invoice_project_xref

            
        )

        
)

select * from v_opco_project_invoice_project_xr_lineage
  );
