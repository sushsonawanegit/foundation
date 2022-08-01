
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_pymnt_terms_lineage 
  
   as (
    with v_opco_pymnt_terms_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_PYMNT_TERMS_CK" as character varying(32)) as "OPCO_PYMNT_TERMS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_PYMNT_TERMS_CD" as character varying(40)) as "SRC_PYMNT_TERMS_CD" ,
                    cast("SRC_PYMNT_TERMS_DESC" as character varying(100)) as "SRC_PYMNT_TERMS_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_pymnt_terms

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_PYMNT_TERMS_CK" as character varying(32)) as "OPCO_PYMNT_TERMS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_PYMNT_TERMS_CD" as character varying(40)) as "SRC_PYMNT_TERMS_CD" ,
                    cast("SRC_PYMNT_TERMS_DESC" as character varying(100)) as "SRC_PYMNT_TERMS_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_pymnt_terms

            
        )

        
)

select * from v_opco_pymnt_terms_lineage
  );
