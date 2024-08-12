{% macro generate_stage_list(stage_name) %}

{# /* run the list command to get a file listing */ #}
{% set res_list %}
    list {{ stage_name }} 
{% endset %}
{% set res = run_query(res_list) %}

{# /* use last_query_id to return a table */ #}
{% set res_list_last_query_id %}
    select * from table(result_scan(last_query_id()))
{% endset %}


{% if execute %}
    {% set result = run_query(res_list_last_query_id) %}
    {% if result|length > 0 %}
        {% set res_list = result.columns[0].values() %}
    {% else %}
        {% set res_list = [] %}
    {% endif %}
{% else %}
    {% set res_list = [] %}
{% endif %}

{# /* build a union of the results */ #}
{% if res_list|length > 0 %}
    {% for i in res_list %}
    select '{{ i }}' as filepath {{ "union " if not loop.last else "" }}
    {% endfor %}
{% else %}
    select '' as filepath 
{% endif %}

{% endmacro %}
