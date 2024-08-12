{{
  generate_flatten_avro(
    model_name = ref('stg_colleague_confidential_avro'),
    source_name = source('people_raw','colleagueconfidential_data_event'),
    json_column = 'record_content'
  )
}}