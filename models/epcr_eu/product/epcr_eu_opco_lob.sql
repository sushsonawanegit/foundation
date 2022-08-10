{% set _load = load_type('EPCR_EU_OPCO_LOB') %}

with epcr_eu_opco_lob as (
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'EPCR-EU'::varchar(20) as src_sys_nm,
    upper(trim(segmentcode))::varchar(15) as src_lob_cd,
    max(segmentname)::varchar(50) as src_lob_nm,
    case 
        when activeflag = 'TRUE' then 1 else 0 
    end::numeric(1,0) as actv_ind,
    {{ spcl_chr_rp('src_lob_nm')}}::varchar(100) as lob_wo_spcl_chr_cd
    from {{source('EPCR_EU_DEV', 'COASEGVALUES')}} 
    where coacode = 'MAIN' and segmentnbr = 5
    and company not in ('CUBAUS01','CUBAUS02') 
    group by src_sys_nm, src_lob_cd , _fivetran_deleted, actv_ind
    		
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_lob_cd']) }} as opco_lob_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_lob_cd', 'src_lob_nm', 'actv_ind']) }} as opco_lob_ck,
            * 
    from epcr_eu_opco_lob
),
delete_captured as (
    select * from final
    {% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'EPCR_EU_OPCO_LOB') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_LOB') and _load != 3%}
        union
        select
        opco_lob_sk, opco_lob_ck, crt_dtm, 
        (select max(stg_load_dtm) from final) as stg_load_dtm,
        (select max(stg_load_dtm) from final) as delete_dtm,
        src_sys_nm, src_lob_cd, src_lob_nm, actv_ind
        from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB
        {% if _load == 1 %}
            where OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from final)
        {% else %}
            where OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from final) 
                and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'EPCR-EU')
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
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_LOB_ck not in (select distinct OPCO_LOB_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'EPCR-EU')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and OPCO_LOB_ck in (select distinct OPCO_LOB_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.OPCO_LOB where src_sys_nm = 'EPCR-EU') and delete_dtm is not null)
    {% endif %}
{% endif %}  