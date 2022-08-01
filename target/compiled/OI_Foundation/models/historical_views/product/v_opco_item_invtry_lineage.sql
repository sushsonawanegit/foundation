with v_opco_item_invtry_lineage as(
    
    

        (
            select

                
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_ITEM_INVTRY_CK" as character varying(32)) as "OPCO_ITEM_INVTRY_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("ITEM_AVBL_PHYSICAL_QTY" as NUMBER(28,12)) as "ITEM_AVBL_PHYSICAL_QTY" ,
                    cast("ITEM_POSTED_QTY" as NUMBER(28,12)) as "ITEM_POSTED_QTY" ,
                    cast("ITEM_POSTED_VALUE_AMT" as NUMBER(28,12)) as "ITEM_POSTED_VALUE_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_invtry

            
        )

        
)

select * from v_opco_item_invtry_lineage