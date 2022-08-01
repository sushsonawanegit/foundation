with v_opco_cash_dscnt_terms_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_CK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_CASH_DSCNT_TERMS_CD" as character varying(20)) as "SRC_CASH_DSCNT_TERMS_CD" ,
                    cast("SRC_CASH_DSCNT_TERMS_DESC" as character varying(100)) as "SRC_CASH_DSCNT_TERMS_DESC" ,
                    cast("PYMNT_PERIOD_IN_DAYS_NBR" as NUMBER(4,0)) as "PYMNT_PERIOD_IN_DAYS_NBR" ,
                    cast("DSCNT_PCT" as FLOAT) as "DSCNT_PCT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cash_dscnt_terms

            
        )

        
)

select * from v_opco_cash_dscnt_terms_lineage