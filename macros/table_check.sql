{% macro table_check(tb_nm1) %}
    {% set present_comp = [] %}
    {% for schm in var('primex_schemas') %}
        {% set myquery %}
            SELECT EXISTS(SELECT * FROM etl_staging_uat.information_schema.tables WHERE table_schema = '{{schm}}' AND table_name = '{{tb_nm1}}')
        {% endset %}
        {% set results=run_query(myquery) %}
        {% if execute %}
            {% set rset=results.columns[0].values()[0] %}
        {% endif %}
        {% if rset %}
            {% do present_comp.append(schm + '.' + tb_nm1 ) %}
        {% endif %}
    {% endfor %}
    {{ return(present_comp) }}
{% endmacro %}