{{
    config(
        materialized='table',
        transient=false
    )
}}

with primex_opco as(
    {% for tbl in var('primex_companies') %}
        select 
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        concat_ws('|', 'SYSPRO-PRMX', '{{ tbl}}') as opco_ak,
        'SYSPRO-PRMX' as src_sys_nm,
        '{{ tbl}}' as opco_id,
        case 
            when '{{ tbl}}' = 'A' then 'Primex Manufacturing Ltd.'
            when '{{ tbl}}' = 'C' then 'Primex Manufacturing Corp'
            when '{{ tbl}}' = 'P' then 'Primex Technologies Inc.'
            when '{{ tbl}}' = 'X' then 'Primex Elimination Co.'
        end as opco_nm,
        (select upper(trim(currency)) as currency_cd from {{ var('primex_db')}}.{{ var('primex_schema')}}.COMPANY{{ tbl}}_DBO_TBLCURRENCY where buyexchangerate = 1) as opco_currency_key,
        case 
            when '{{ tbl}}' = 'A' then '2019-07-07'::date
            when '{{ tbl}}' = 'C' then '2020-03-11'::date
            when '{{ tbl}}' = 'X' then '2019-07-07'::date
            else null
        end as acqstn_dt,
        iff('{{ tbl}}' in ('A', 'C', 'X'), 1, 0)::number(1,0) as actv_ind,
        case 
            when '{{ tbl}}' = 'P' then '2022-01-01'::date
            else null
        end as inactv_dt
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['o.src_sys_nm', 'opco_id']) }} as opco_sk,
            {{ dbt_utils.surrogate_key(['o.src_sys_nm', 'opco_id', 'opco_nm', 'acqstn_dt', 'actv_ind', 'inactv_dt']) }} as opco_ck,
            o.opco_ak, o.crt_dtm, o.stg_load_dtm, o.delete_dtm, o.src_sys_nm, o.opco_id, o.opco_nm, o.acqstn_dt, o.actv_ind, o.inactv_dt, oc.opco_currency_sk
    from primex_opco o
    left join {{ ref('opco_currency')}} oc 
        on oc.src_currency_cd = opco_currency_key
        and oc.src_sys_nm = 'SYSPRO-PRMX'
)

select * from final