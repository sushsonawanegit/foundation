{% set _load = load_type('AX_OPCO_PYMNT_TERMS') %}

with ax_opco_pymnt_terms as(
    select
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(paymtermid)::varchar(40) as src_pymnt_terms_cd,
    max(description)::varchar(100) as src_pymnt_terms_desc
    from {{source('AX_DEV', 'PAYMTERM')}}
    where dataareaid not in {{ var('excluded_ax_companies')}}
    group by src_sys_nm, src_pymnt_terms_cd, _fivetran_deleted
),
final as(
    select  {{dbt_utils.surrogate_key(['src_sys_nm', 'src_pymnt_terms_cd'])}} as opco_pymnt_terms_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_pymnt_terms_cd','src_pymnt_terms_desc']) }} as opco_pymnt_terms_ck, 
            *
    from ax_opco_pymnt_terms 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PYMNT_TERMS') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PYMNT_TERMS') and _load != 3%}
    union
    select
    opco_pymnt_terms_sk, opco_pymnt_terms_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, src_pymnt_terms_cd, src_pymnt_terms_desc
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms
    {% if _load == 1 %}
        where opco_pymnt_terms_ck not in (select distinct opco_pymnt_terms_ck from final)
    {% else %}
        where opco_pymnt_terms_ck not in (select distinct opco_pymnt_terms_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_pymnt_terms_ck not in (select distinct opco_pymnt_terms_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_pymnt_terms_ck in (select distinct opco_pymnt_terms_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_pymnt_terms where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}