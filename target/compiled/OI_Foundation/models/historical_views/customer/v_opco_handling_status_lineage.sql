with v_opco_handling_status_lineage as(
    
    

        (
            select

                
                    cast("OPCO_HANDLING_STATUS_SK" as character varying(32)) as "OPCO_HANDLING_STATUS_SK" ,
                    cast("OPCO_HANDLING_STATUS_CK" as character varying(32)) as "OPCO_HANDLING_STATUS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("HANDLING_STATUS_CD" as NUMBER(2,0)) as "HANDLING_STATUS_CD" ,
                    cast("HANDLING_STATUS_DESC" as character varying(10)) as "HANDLING_STATUS_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_handling_status

            
        )

        
)

select * from v_opco_handling_status_lineage