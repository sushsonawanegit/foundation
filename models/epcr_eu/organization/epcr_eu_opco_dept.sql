{% set _load = load_type('EPCR_EU_OPCO_DEPT') %}

with epcr_eu_opco_dept as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'EPCR-EU' as src_sys_nm,
    upper(trim(segmentcode)) as src_dept_cd,
    max(segmentname) as src_dept_desc,
    case 
        when activeflag =  'TRUE' then 1
        else 0
    end as actv_ind,
    {{ spcl_chr_rp('src_dept_cd')}} as dept_wo_spcl_chr_cd
    from {{source('EPCR_EU_DEV', 'COASEGVALUES')}}
    where coacode = 'MAIN' and segmentnbr = 3
    and company not in ('CUBAUS01','CUBAUS02') 
    group by src_sys_nm, src_dept_cd , _fivetran_deleted, actv_ind

),
final as(
    select 
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_dept_cd'])}} as opco_dept_sk,
        {{dbt_utils.surrogate_key(['src_sys_nm', 'src_dept_cd','src_dept_desc','actv_ind','dept_wo_spcl_chr_cd' ])}} as opco_dept_ck, 
        *
    from epcr_eu_opco_dept

),
delete_captured as(
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_DEPT') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_DEPT') and _load != 3%}
        union
        select
        opco_dept_sk, opco_dept_ck, CRT_DTM, 
        (select MAX(STG_LOAD_DTM) from final) as STG_LOAD_DTM,
        (select MAX(STG_LOAD_DTM) from final) as DELETE_DTM,
        src_sys_nm, src_dept_cd, src_dept_desc, actv_ind, dept_wo_spcl_chr_cd
         
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_dept
        {% if _load == 1 %}
            where opco_dept_ck not in (select distinct opco_dept_ck from final)
        {% else %}
            where opco_dept_ck not in (select distinct opco_dept_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_dept where src_sys_nm = 'EPCR-EU')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dept_ck not in (select distinct opco_dept_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_dept where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_dept_ck in (select distinct opco_dept_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_dept where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}
