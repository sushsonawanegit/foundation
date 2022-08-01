
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_vendor_lineage 
  
   as (
    with v_opco_vendor_lineage as(
    
    

        (
            select

                
                    cast("OPCO_VENDOR_SK" as character varying(32)) as "OPCO_VENDOR_SK" ,
                    cast("OPCO_VENDOR_CK" as character varying(32)) as "OPCO_VENDOR_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("VENDOR_ID" as character varying(120)) as "VENDOR_ID" ,
                    cast("VENDOR_NM" as character varying(120)) as "VENDOR_NM" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("OPCO_CURRENCY_SK" as character varying(32)) as "OPCO_CURRENCY_SK" ,
                    cast("VENDOR_GRP_CD" as character varying(1)) as "VENDOR_GRP_CD" ,
                    cast("VENDOR_ALT_NM" as character varying(60)) as "VENDOR_ALT_NM" ,
                    cast("VENDOR_DBA_NM" as character varying(120)) as "VENDOR_DBA_NM" ,
                    cast("FOREIGN_ENTITY_IND" as NUMBER(38,0)) as "FOREIGN_ENTITY_IND" ,
                    cast("VENDOR_BLOCK_TXT" as character varying(16777216)) as "VENDOR_BLOCK_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor

            
        )

        
)

select * from v_opco_vendor_lineage
  );
