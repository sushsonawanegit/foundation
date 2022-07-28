{% macro table_check(tb_nm1, tb_nm2) %}
    {% set present_comp = [] %}
    {% for tbl in var('primex_companies') %}
        {% set myquery %}
            SELECT EXISTS(SELECT * FROM mule_staging.information_schema.tables WHERE table_schema = 'SYSPRO_PRIMEX' AND table_name = '{{ tb_nm1}}{{ tbl}}{{ tb_nm2}}')
        {% endset %}
        {% set results=run_query(myquery) %}
        {% if execute %}
            {% set rset=results.columns[0].values()[0] %}
        {% endif %}
        {% if rset %}
            {% do present_comp.append(tbl) %}
        {% endif %}
    {% endfor %}
    {{ return(present_comp) }}
{% endmacro %}