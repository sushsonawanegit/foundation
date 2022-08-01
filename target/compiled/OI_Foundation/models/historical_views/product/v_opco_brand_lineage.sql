with v_opco_brand as(
    
    

        (
            select

                
                    cast("OPCO_BRAND_SK" as character varying(32)) as "OPCO_BRAND_SK" ,
                    cast("OPCO_BRAND_CK" as character varying(32)) as "OPCO_BRAND_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_BRAND_CD" as character varying(20)) as "SRC_BRAND_CD" ,
                    cast("SRC_BRAND_NM" as character varying(100)) as "SRC_BRAND_NM" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("BRAND_WO_SPCL_CHR_CD" as character varying(100)) as "BRAND_WO_SPCL_CHR_CD" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_brand

            
        )

        
)

select * from v_opco_brand