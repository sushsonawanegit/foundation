with v_opco_cust_settled_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_SETTLED_TRANS_SK" as character varying(32)) as "OPCO_CUST_SETTLED_TRANS_SK" ,
                    cast("OPCO_CUST_SETTLED_TRANS_CK" as character varying(32)) as "OPCO_CUST_SETTLED_TRANS_CK" ,
                    cast("OPCO_CUST_SETTLED_TRANS_AK" as character varying(16777216)) as "OPCO_CUST_SETTLED_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CUST_TRANS_SK" as character varying(32)) as "OPCO_CUST_TRANS_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("CASH_DSCNT_GL_ACCT_SK" as character varying(32)) as "CASH_DSCNT_GL_ACCT_SK" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("DUE_DT" as TIMESTAMP_NTZ) as "DUE_DT" ,
                    cast("SRC_CRT_DTM" as TIMESTAMP_NTZ) as "SRC_CRT_DTM" ,
                    cast("OFFSET_CUST_SK" as character varying(32)) as "OFFSET_CUST_SK" ,
                    cast("OFFSET_CUST_TRANS_SK" as character varying(32)) as "OFFSET_CUST_TRANS_SK" ,
                    cast("OFFSET_VOUCHER_NBR" as character varying(40)) as "OFFSET_VOUCHER_NBR" ,
                    cast("CUST_CASH_DSCNT_DT" as TIMESTAMP_NTZ) as "CUST_CASH_DSCNT_DT" ,
                    cast("LATEST_INTEREST_CALC_DT" as TIMESTAMP_NTZ) as "LATEST_INTEREST_CALC_DT" ,
                    cast("REVERSIBLE_IND" as NUMBER(38,0)) as "REVERSIBLE_IND" ,
                    cast("SETTLEMENT_GRP_TXT" as NUMBER(38,0)) as "SETTLEMENT_GRP_TXT" ,
                    cast("SETTLED_VOUCHER_NBR" as character varying(40)) as "SETTLED_VOUCHER_NBR" ,
                    cast("TRANS_CURRENCY_SETTLED_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_SETTLED_AMT" ,
                    cast("OPCO_CURRENCY_SETTLED_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SETTLED_AMT" ,
                    cast("EXCH_ADJSTMNT_AMT" as NUMBER(28,12)) as "EXCH_ADJSTMNT_AMT" ,
                    cast("UTILIZED_CASH_DSCNT_AMT" as NUMBER(28,12)) as "UTILIZED_CASH_DSCNT_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_settled_trans

            
        )

        
)

select * from v_opco_cust_settled_trans_lineage