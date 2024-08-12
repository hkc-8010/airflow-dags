{{
  generate_flatten_avro(
    model_name = ref('stg_orgunit_avro'),
    source_name = source('people_raw','orgunit_data_event'),
    json_column = 'record_content'
  )
}}