
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_sub_ledger_type_lineage 
  
   as (
    with v_opco_sub_ledger_type as(
    
    

        (
            select

                
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_CK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_SUB_LEDGER_TYPE_CD" as character varying(20)) as "SRC_SUB_LEDGER_TYPE_CD" ,
                    cast("SRC_SUB_LEDGER_TYPE_DESC" as character varying(50)) as "SRC_SUB_LEDGER_TYPE_DESC" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast(null as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast(null as TIMESTAMP_TZ) as "DELETE_DTM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_sub_ledger_type

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_CK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_SUB_LEDGER_TYPE_CD" as character varying(20)) as "SRC_SUB_LEDGER_TYPE_CD" ,
                    cast("SRC_SUB_LEDGER_TYPE_DESC" as character varying(50)) as "SRC_SUB_LEDGER_TYPE_DESC" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_sub_ledger_type

            
        )

        
)

select * from v_opco_sub_ledger_type
  );
