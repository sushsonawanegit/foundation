begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_commsn_grp ("OPCO_COMMSN_GRP_SK", "OPCO_COMMSN_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_COMMSN_GRP_CD", "SRC_COMMSN_GRP_DESC", "SRC_COMMSN_GRP_TYPE_TXT")
        (
            select "OPCO_COMMSN_GRP_SK", "OPCO_COMMSN_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_COMMSN_GRP_CD", "SRC_COMMSN_GRP_DESC", "SRC_COMMSN_GRP_TYPE_TXT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_commsn_grp__dbt_tmp
        );
    commit;