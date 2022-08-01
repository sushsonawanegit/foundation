{% set _load = load_type('AX_OPCO_PROJECT_REVENUE_TRANS') %}

with ax_opco_project_revenue_trans as (
    select
    current_timestamp as crt_dtm,
    pro._fivetran_synced as stg_load_dtm,
    case
        when pro._fivetran_deleted = true then pro._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    pro.recid as src_key_txt,
    opr.opco_project_sk,
    opco.opco_sk,
    pro.changeno_opi as chg_nbr,
    pro.projrevenuetype_opi as project_revenue_trans_type_cd,
    pro.comment_opi as project_revenue_trans_desc,
    pro.transdate_opi as trans_dt,
    pro.approved_opi as apprvd_ind,
    pro.projrevenueamount_opi as project_revenue_trans_amt
    from {{ source('AX_DEV', 'PROJREVENUE_OPI') }} pro
    left join {{ ref('ax_opco_project_curr')}} opr
        on pro.dataareaid = opr.opco_id
        and pro.projid_opi = opr.project_id
        and opr.src_sys_nm = 'AX'
    left join {{ ref('ax_opco_curr')}} opco
        on pro.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX'
    where pro.dataareaid not in {{ var('excluded_ax_companies')}}
),
final as (															
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm','src_key_txt']) }} as opco_project_revenue_trans_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_key_txt', 'opco_project_sk', 'opco_sk', 'chg_nbr', 'project_revenue_trans_type_cd', 'project_revenue_trans_desc', 'trans_dt', 'apprvd_ind', 'project_revenue_trans_amt']) }} as opco_project_revenue_trans_ck,
        concat_ws('|', src_sys_nm, src_key_txt) as opco_project_revenue_trans_ak,
        * 		
    from ax_opco_project_revenue_trans 															
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_REVENUE_TRANS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_REVENUE_TRANS') and _load != 3%}
    union
    select
    OPCO_PROJECT_REVENUE_TRANS_sk, OPCO_PROJECT_REVENUE_TRANS_ck,OPCO_PROJECT_REVENUE_TRANS_AK ,crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_key_txt, opco_project_sk, opco_sk, chg_nbr, project_revenue_trans_type_cd, project_revenue_trans_desc, trans_dt, apprvd_ind, project_revenue_trans_amt
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_REVENUE_TRANS
    {% if _load == 1 %}
        where OPCO_PROJECT_REVENUE_TRANS_ck not in (select distinct OPCO_PROJECT_REVENUE_TRANS_ck from final)
    {% else %}
        where OPCO_PROJECT_REVENUE_TRANS_ck not in (select distinct OPCO_PROJECT_REVENUE_TRANS_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_REVENUE_TRANS where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_REVENUE_TRANS_ck not in (select distinct OPCO_PROJECT_REVENUE_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_REVENUE_TRANS where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PROJECT_REVENUE_TRANS_ck in (select distinct OPCO_PROJECT_REVENUE_TRANS_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PROJECT_REVENUE_TRANS where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}