begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept ("OPCO_DEPT_SK", "OPCO_DEPT_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DEPT_CD", "SRC_DEPT_DESC", "DEPT_WO_SPCL_CHR_CD", "ACTV_IND")
        (
            select "OPCO_DEPT_SK", "OPCO_DEPT_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DEPT_CD", "SRC_DEPT_DESC", "DEPT_WO_SPCL_CHR_CD", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept__dbt_tmp
        );
    commit;