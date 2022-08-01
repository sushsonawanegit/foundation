with v_opco_cust_code_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_CODE_SK" as character varying(32)) as "OPCO_CUST_CODE_SK" ,
                    cast("OPCO_CUST_CODE_CK" as character varying(32)) as "OPCO_CUST_CODE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_CUST_CODE_CD" as character varying(90)) as "SRC_CUST_CODE_CD" ,
                    cast("SRC_CUST_CODE_DESC" as character varying(60)) as "SRC_CUST_CODE_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_code

            
        )

        
)

select * from v_opco_cust_code_lineage