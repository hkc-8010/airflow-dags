{{
    config(
        materialized='table'
    )
}}

{{ parse_avro('people.position.data.event-value.avsc') }}

 -- depends_on: {{ ref('dbt_results') }}