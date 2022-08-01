with v_opco_item_freight_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ITEM_FREIGHT_SK" as character varying(32)) as "OPCO_ITEM_FREIGHT_SK" ,
                    cast("OPCO_ITEM_FREIGHT_CK" as character varying(32)) as "OPCO_ITEM_FREIGHT_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_ITEM_FREIGHT_CD" as character varying(120)) as "SRC_ITEM_FREIGHT_CD" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("SRC_ITEM_FREIGHT_NM" as character varying(240)) as "SRC_ITEM_FREIGHT_NM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_freight

            
        )

        
)

select * from v_opco_item_freight_lineage