begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans_type ("OPCO_GL_TRANS_TYPE_SK", "OPCO_GL_TRANS_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_GL_TRANS_TYPE_CD", "SRC_GL_TRANS_TYPE_DESC")
        (
            select "OPCO_GL_TRANS_TYPE_SK", "OPCO_GL_TRANS_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_GL_TRANS_TYPE_CD", "SRC_GL_TRANS_TYPE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans_type__dbt_tmp
        );
    commit;