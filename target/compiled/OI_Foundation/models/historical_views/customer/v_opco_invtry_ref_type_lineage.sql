with v_opco_invtry_ref_type_lineage as(
    
    

        (
            select

                
                    cast("OPCO_INVTRY_REF_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_REF_TYPE_SK" ,
                    cast("OPCO_INVTRY_REF_TYPE_CK" as character varying(32)) as "OPCO_INVTRY_REF_TYPE_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("INVTRY_REF_TYPE_CD" as NUMBER(1,0)) as "INVTRY_REF_TYPE_CD" ,
                    cast("INVTRY_REF_TYPE_DESC" as character varying(17)) as "INVTRY_REF_TYPE_DESC" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_ref_type

            
        )

        
)

select * from v_opco_invtry_ref_type_lineage