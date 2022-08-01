with v_fscl_clndr as(
    
    

        (
            select

                
                    cast("FSCL_CLNDR_SK" as character varying(32)) as "FSCL_CLNDR_SK" ,
                    cast("FSCL_CLNDR_CK" as character varying(32)) as "FSCL_CLNDR_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("BUSN_APPRVD_IND" as NUMBER(1,0)) as "BUSN_APPRVD_IND" ,
                    cast("CLNDR_DT" as DATE) as "CLNDR_DT" ,
                    cast("DT_AK" as NUMBER(38,0)) as "DT_AK" ,
                    cast("SERIAL_MNTH_NBR" as NUMBER(38,0)) as "SERIAL_MNTH_NBR" ,
                    cast("FSCL_YR_ID" as NUMBER(38,0)) as "FSCL_YR_ID" ,
                    cast("FSCL_YR_NM" as character varying(48)) as "FSCL_YR_NM" ,
                    cast("FSCL_YR_MNTH_ID" as NUMBER(38,0)) as "FSCL_YR_MNTH_ID" ,
                    cast("FSCL_YR_MNTH_NM" as character varying(48)) as "FSCL_YR_MNTH_NM" ,
                    cast("FSCL_MNTH_NBR" as NUMBER(38,0)) as "FSCL_MNTH_NBR" ,
                    cast("FSCL_MNTH_NM" as character varying(16777216)) as "FSCL_MNTH_NM" ,
                    cast("CN_TO_US_CNVRSN_RT" as FLOAT) as "CN_TO_US_CNVRSN_RT" ,
                    cast("FSCL_MNTH_STRT_DT" as DATE) as "FSCL_MNTH_STRT_DT" ,
                    cast("FSCL_MNTH_END_DT" as DATE) as "FSCL_MNTH_END_DT" ,
                    cast("PREV_FSCL_MNTH_STRT_DT" as DATE) as "PREV_FSCL_MNTH_STRT_DT" ,
                    cast("PREV_FSCL_MNTH_END_DT" as DATE) as "PREV_FSCL_MNTH_END_DT" ,
                    cast("FSCL_YR_STRT_DT" as DATE) as "FSCL_YR_STRT_DT" ,
                    cast("FSCL_YR_END_DT" as DATE) as "FSCL_YR_END_DT" ,
                    cast("FSCL_MNTH_DAYS_NBR" as NUMBER(38,0)) as "FSCL_MNTH_DAYS_NBR" ,
                    cast("FSCL_PERIOD_ID" as character varying(44)) as "FSCL_PERIOD_ID" ,
                    cast("FSCL_WK_DAY_NBR" as NUMBER(38,0)) as "FSCL_WK_DAY_NBR" ,
                    cast("WK_DAY_IND" as BOOLEAN) as "WK_DAY_IND" ,
                    cast("CLNDR_YR_NBR" as NUMBER(38,0)) as "CLNDR_YR_NBR" ,
                    cast("CLNDR_YR_DAY_NBR" as NUMBER(38,0)) as "CLNDR_YR_DAY_NBR" ,
                    cast("CLNDR_QTR_NBR" as NUMBER(38,0)) as "CLNDR_QTR_NBR" ,
                    cast("CLNDR_YR_QTR_NBR" as NUMBER(38,0)) as "CLNDR_YR_QTR_NBR" ,
                    cast("CLNDR_QTR_DAY_NBR" as NUMBER(38,0)) as "CLNDR_QTR_DAY_NBR" ,
                    cast("CLNDR_QTR_MNTH_NBR" as NUMBER(38,0)) as "CLNDR_QTR_MNTH_NBR" ,
                    cast("CLNDR_MNTH_NBR" as NUMBER(38,0)) as "CLNDR_MNTH_NBR" ,
                    cast("CLNDR_YR_MNTH_NBR" as NUMBER(38,0)) as "CLNDR_YR_MNTH_NBR" ,
                    cast("CLNDR_MNTH_DAY_NBR" as NUMBER(38,0)) as "CLNDR_MNTH_DAY_NBR" ,
                    cast("CLNDR_WK_YR_NBR" as NUMBER(38,0)) as "CLNDR_WK_YR_NBR" ,
                    cast("CLNDR_YR_WK_DAY_NBR" as NUMBER(38,0)) as "CLNDR_YR_WK_DAY_NBR" ,
                    cast("CLNDR_WK_NBR" as NUMBER(38,0)) as "CLNDR_WK_NBR" ,
                    cast("CLNDR_YR_WK_NBR" as NUMBER(38,0)) as "CLNDR_YR_WK_NBR" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("FSCL_YR_DAY_NBR" as NUMBER(38,0)) as "FSCL_YR_DAY_NBR" ,
                    cast("FSCL_QTR_NBR" as NUMBER(38,0)) as "FSCL_QTR_NBR" ,
                    cast("FSCL_YR_QTR_NBR" as NUMBER(38,0)) as "FSCL_YR_QTR_NBR" ,
                    cast("FSCL_QTR_DAY_NBR" as NUMBER(38,0)) as "FSCL_QTR_DAY_NBR" ,
                    cast("FSCL_QTR_MNTH_NBR" as NUMBER(38,0)) as "FSCL_QTR_MNTH_NBR" ,
                    cast("FSCL_YR_MNTH_NBR" as NUMBER(38,0)) as "FSCL_YR_MNTH_NBR" ,
                    cast("FSCL_MNTH_NBR_1" as NUMBER(38,0)) as "FSCL_MNTH_NBR_1" ,
                    cast("FSCL_MNTH_DAY_NBR" as NUMBER(38,0)) as "FSCL_MNTH_DAY_NBR" ,
                    cast("FSCL_WK_YR_NBR" as NUMBER(38,0)) as "FSCL_WK_YR_NBR" ,
                    cast("FSCL_YR_WK_DAY_NBR" as NUMBER(38,0)) as "FSCL_YR_WK_DAY_NBR" ,
                    cast("FSCL_YR_WK_NBR" as NUMBER(38,0)) as "FSCL_YR_WK_NBR" ,
                    cast("FSCL_WK_NBR" as NUMBER(38,0)) as "FSCL_WK_NBR" ,
                    cast("DT_CAPTION_1033_TXT" as character varying(1020)) as "DT_CAPTION_1033_TXT" ,
                    cast("WK_DAY_CAPTION_1033_TXT" as character varying(1020)) as "WK_DAY_CAPTION_1033_TXT" ,
                    cast("CLNDR_YR_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_YR_CAPTION_1033_TXT" ,
                    cast("CLNDR_QTR_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_QTR_CAPTION_1033_TXT" ,
                    cast("CLNDR_YR_QTR_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_YR_QTR_CAPTION_1033_TXT" ,
                    cast("CLNDR_MNTH_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_MNTH_CAPTION_1033_TXT" ,
                    cast("CLNDR_YR_MNTH_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_YR_MNTH_CAPTION_1033_TXT" ,
                    cast("CLNDR_WK_YR_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_WK_YR_CAPTION_1033_TXT" ,
                    cast("CLNDR_WK_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_WK_CAPTION_1033_TXT" ,
                    cast("CLNDR_YR_WK_CAPTION_1033_TXT" as character varying(1020)) as "CLNDR_YR_WK_CAPTION_1033_TXT" ,
                    cast("FSCL_YR_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_YR_CAPTION_1033_TXT" ,
                    cast("FSCL_QTR_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_QTR_CAPTION_1033_TXT" ,
                    cast("FSCL_YR_QTR_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_YR_QTR_CAPTION_1033_TXT" ,
                    cast("FSCL_MNTH_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_MNTH_CAPTION_1033_TXT" ,
                    cast("FSCL_YR_MNTH_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_YR_MNTH_CAPTION_1033_TXT" ,
                    cast("FSCL_WK_YR_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_WK_YR_CAPTION_1033_TXT" ,
                    cast("FSCL_YR_WK_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_YR_WK_CAPTION_1033_TXT" ,
                    cast("FSCL_WK_CAPTION_1033_TXT" as character varying(1020)) as "FSCL_WK_CAPTION_1033_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_fscl_clndr

            
        )

        
)

select * from v_fscl_clndr