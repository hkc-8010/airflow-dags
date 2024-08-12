{% macro get_relation_columns(model, column_alias, column_suffix, last_trailing_comma) %}

    {%- set column_list = [] -%}

    {% for col in adapter.get_columns_in_relation(ref(model)) -%}

        {%- if (col.column | lower not in('dbt_valid_from','dbt_valid_to','dbt_updated_at','created_at','modified_at','dbt_model_run_id')) and (not (col.column | lower).endswith('_key'))  %}

            {%- do column_list.append(column_alias + "." + col.column + " as " + column_suffix + "_" + col.column) -%}

            {%- if not loop.last %}
                {%- do column_list.append(", ") -%}
            {%- endif -%}
            {%- if loop.last and last_trailing_comma %}
                {%- do column_list.append(", ") -%}
            {%- endif -%}
        {%- endif -%}

    {% endfor %}

     {{return(column_list | join('') | lower)}}

{% endmacro %}

