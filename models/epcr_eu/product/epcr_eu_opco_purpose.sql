

{% set _load = load_type('EPCR_EU_OPCO_PURPOSE') %}

with epcr_eu_opco_purpose as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,

    'EPCR-EU'::varchar(20) as src_sys_nm,
    upper(trim(segmentcode))::varchar(20) as src_purpose_cd,
    max(segmentcode)::varchar(100) as src_purpose_desc,
    case 
        when activeflag = 'T' then 1 
        else 0 
    end::numeric(1,0) as actv_ind,
    {{ spcl_chr_rp('src_purpose_desc')}}::varchar(100) as purpose_wo_spcl_chr_cd
    from {{source('EPCR_EU_DEV', 'COASEGVALUES')}}
    group by src_sys_nm, src_purpose_cd , _fivetran_deleted, actv_ind
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd']) }} as opco_purpose_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_purpose_cd', 'src_purpose_desc', 'purpose_wo_spcl_chr_cd', 'actv_ind']) }} as opco_purpose_ck,
            * 
    from epcr_eu_opco_purpose 
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_PURPOSE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PURPOSE') and _load != 3%}
        union
        select
        opco_PURPOSE_sk, opco_PURPOSE_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_PURPOSE_cd, src_PURPOSE_nm, actv_ind
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE
        {% if _load == 1 %}
            where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final)
        {% else %}
            where OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'EPCR-EU')
        {% endif %} 
        and src_sys_nm = 'EPCR-EU'
    {% endif %}
)

select * from delete_captured

{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck not in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_PURPOSE_ck in (select distinct OPCO_PURPOSE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_PURPOSE where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}  