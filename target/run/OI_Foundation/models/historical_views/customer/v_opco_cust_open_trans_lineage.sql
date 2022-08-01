
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_open_trans_lineage 
  
   as (
    with v_opco_cust_open_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_OPEN_TRANS_SK" as character varying(32)) as "OPCO_CUST_OPEN_TRANS_SK" ,
                    cast("OPCO_CUST_OPEN_TRANS_CK" as character varying(32)) as "OPCO_CUST_OPEN_TRANS_CK" ,
                    cast("OPCO_CUST_OPEN_TRANS_AK" as character varying(16777216)) as "OPCO_CUST_OPEN_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_CUST_TRANS_SK" as character varying(32)) as "OPCO_CUST_TRANS_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("CASH_DSCNT_GL_ACCT_SK" as character varying(32)) as "CASH_DSCNT_GL_ACCT_SK" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("DUE_DT" as TIMESTAMP_NTZ) as "DUE_DT" ,
                    cast("LTST_INTEREST_CALC_DT" as TIMESTAMP_NTZ) as "LTST_INTEREST_CALC_DT" ,
                    cast("CASH_DSCNT_DT" as TIMESTAMP_NTZ) as "CASH_DSCNT_DT" ,
                    cast("CASH_DSCNT_USAGE_TXT" as character varying(16777216)) as "CASH_DSCNT_USAGE_TXT" ,
                    cast("TRANS_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_TRANS_AMT" ,
                    cast("OPCO_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TRANS_AMT" ,
                    cast("INCLUDED_CASH_DSCNT_AMT" as NUMBER(28,12)) as "INCLUDED_CASH_DSCNT_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_open_trans

            
        )

        
)

select * from v_opco_cust_open_trans_lineage
  );
