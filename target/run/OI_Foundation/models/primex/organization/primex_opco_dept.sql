begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_dept ("OPCO_DEPT_SK", "OPCO_DEPT_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DEPT_CD", "SRC_DEPT_DESC", "ACTV_IND", "DEPT_WO_SPCL_CHR_CD")
        (
            select "OPCO_DEPT_SK", "OPCO_DEPT_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DEPT_CD", "SRC_DEPT_DESC", "ACTV_IND", "DEPT_WO_SPCL_CHR_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_dept__dbt_tmp
        );
    commit;