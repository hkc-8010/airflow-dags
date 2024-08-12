{{
  generate_flatten_avro(
    model_name = ref('stg_colleague_avro'),
    source_name = source('people_raw','colleague_data_event'),
    json_column = 'record_content'
  )
}}