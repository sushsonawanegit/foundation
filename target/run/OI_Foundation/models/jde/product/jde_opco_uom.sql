begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_uom ("OPCO_UOM_SK", "OPCO_UOM_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_UOM_CD", "SRC_UOM_DESC")
        (
            select "OPCO_UOM_SK", "OPCO_UOM_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_UOM_CD", "SRC_UOM_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_uom__dbt_tmp
        );
    commit;