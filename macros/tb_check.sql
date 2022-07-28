{% macro tb_check(db_nm, sch_nm, tb_nm) %}

    {% set myquery %}
        SELECT EXISTS(SELECT * FROM {{db_nm}}.information_schema.tables WHERE table_schema = '{{sch_nm}}' AND table_name = '{{tb_nm}}')
    {% endset %}
    {% set results=run_query(myquery) %}
    {% if execute %}
        {% set rset=results.columns[0].values()[0] %}
    {% endif %}

    {{ return(rset)}}

{% endmacro %}