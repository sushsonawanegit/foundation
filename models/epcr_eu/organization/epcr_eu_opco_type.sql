{% set _load = load_type('EPCR_EU_OPCO_TYPE') %}

with epcr_eu_opco_type as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,

    'EPCR-EU'::varchar(20) as src_sys_nm,
    upper(trim(segmentcode))::varchar(20) as src_type_cd,
    max(segmentname)::varchar(100) as src_type_desc,
    case
        when activeflag = 'TRUE' then 1
        else 0
    end::numeric(1,0) as actv_ind,
    {{ spcl_chr_rp('src_type_desc')}}::varchar(100) as type_wo_spcl_chr_cd
    from {{source('EPCR_EU_DEV', 'COASEGVALUES')}} 
    where coacode = 'MAIN' and segmentnbr = 1
    and company not in ('CUBAUS01','CUBAUS02') 
    group by src_sys_nm, src_type_cd , _fivetran_deleted, actv_ind
   
),
final as (
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_TYPE_cd']) }} as opco_TYPE_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_TYPE_cd', 'src_TYPE_desc', 'actv_ind']) }} as opco_TYPE_ck,
            *
    from epcr_eu_opco_type
),

delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_TYPE') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_TYPE') and _load != 3%}
        union
        select
        opco_TYPE_sk, opco_TYPE_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_TYPE_cd, src_TYPE_nm, actv_ind
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE
        {% if _load == 1 %}
            where OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from final)
        {% else %}
            where OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'EPCR-EU')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_TYPE_ck not in (select distinct OPCO_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_TYPE_ck in (select distinct OPCO_TYPE_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_TYPE where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}  