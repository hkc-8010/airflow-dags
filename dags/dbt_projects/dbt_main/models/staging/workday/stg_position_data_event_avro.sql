{{
  generate_flatten_avro(
    model_name = ref('stg_position_avro'),
    source_name = source('people_raw','position_data_event'),
    json_column = 'record_content'
  )
}}