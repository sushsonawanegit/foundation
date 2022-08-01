with v_opco_assctn_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_ASSCTN_CK" as character varying(32)) as "OPCO_ASSCTN_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_ASSCTN_CD" as character varying(80)) as "SRC_ASSCTN_CD" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_SITE_SK" as character varying(32)) as "OPCO_SITE_SK" ,
                    cast("COST_CENTER_SK" as character varying(32)) as "COST_CENTER_SK" ,
                    cast("WAREHOUSE_SK" as character varying(32)) as "WAREHOUSE_SK" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn

            
        )

        
)

select * from v_opco_assctn_lineage