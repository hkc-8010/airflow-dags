{{
  generate_flatten_json(
    model_name = source('people_raw','position_data_event'),
    json_column = 'record_content'
  )
}}

 -- depends_on: {{ ref('dbt_results') }}