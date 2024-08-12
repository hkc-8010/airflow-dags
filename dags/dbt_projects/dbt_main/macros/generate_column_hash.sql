{% macro generate_column_hash(column_list) %}

    {%- set hash_column_list = [] -%}

    {%- for col in column_list -%}
        {%- do hash_column_list.append(
            "coalesce(cast(" ~ col ~ " as string), '')"
        ) -%}

        {%- if not loop.last %}
            {%- do hash_column_list.append(" || '-' || ") -%}
        {%- endif -%}
    {%- endfor -%}

{{ dbt.hash(hash_column_list | join('') ) }}

{% endmacro %}