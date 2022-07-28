{% set _load = load_type('AX_OPCO_COMMSN_GRP') %}

with ax_opco_commsn_grp as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(15) as src_sys_nm,
    upper(groupid)::varchar(50) as src_commsn_grp_cd,
    max(name)::varchar(100) as src_commsn_grp_desc,
    'CUST'::varchar(20) as src_commsn_grp_type_txt
    from {{source('AX_DEV', 'COMMISSIONCUSTOMERGROUP')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_commsn_grp_type_txt, src_commsn_grp_cd, _fivetran_deleted
),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_commsn_grp_cd'])}} as opco_commsn_grp_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_commsn_grp_cd', 'src_commsn_grp_desc','src_commsn_grp_type_txt'])}} as opco_commsn_grp_ck, 
        *
    from ax_opco_COMMSN_GRP
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_COMMSN_GRP') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_COMMSN_GRP') and _load != 3%}
    union
    select
    opco_commsn_grp_sk, opco_commsn_grp_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_commsn_grp_cd, src_commsn_grp_desc, src_commsn_grp_type_txt 
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_commsn_grp
    {% if _load == 1 %}
        where opco_commsn_grp_ck not in (select distinct opco_commsn_grp_ck from final)
    {% else %}
        where opco_commsn_grp_ck not in (select distinct opco_commsn_grp_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_commsn_grp where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_COMMSN_GRP_ck not in (select distinct opco_commsn_grp_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_commsn_grp where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_COMMSN_GRP_ck in (select distinct opco_commsn_grpck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_commsn_grp where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}