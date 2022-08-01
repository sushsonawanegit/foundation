with v_opco_cust_grp_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_GRP_SK" as character varying(32)) as "OPCO_CUST_GRP_SK" ,
                    cast("OPCO_CUST_GRP_CK" as character varying(32)) as "OPCO_CUST_GRP_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_CUST_GRP_CD" as character varying(60)) as "SRC_CUST_GRP_CD" ,
                    cast("SRC_CUST_GRP_DESC" as character varying(120)) as "SRC_CUST_GRP_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_grp

            
        )

        
)

select * from v_opco_cust_grp_lineage