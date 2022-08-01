with v_opco_item_size_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ITEM_SIZE_SK" as character varying(32)) as "OPCO_ITEM_SIZE_SK" ,
                    cast("OPCO_ITEM_SIZE_CK" as character varying(32)) as "OPCO_ITEM_SIZE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_ITEM_SIZE_CD" as character varying(144)) as "SRC_ITEM_SIZE_CD" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("SRC_ITEM_CD" as character varying(80)) as "SRC_ITEM_CD" ,
                    cast("SRC_ITEM_SK" as character varying(32)) as "SRC_ITEM_SK" ,
                    cast("SRC_ITEM_SIZE_NM" as character varying(240)) as "SRC_ITEM_SIZE_NM" ,
                    cast("SRC_ITEM_SIZE_DESC" as character varying(16777216)) as "SRC_ITEM_SIZE_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_size

            
        )

        
)

select * from v_opco_item_size_lineage