{{
    config(
        materialized='table'
    )
}}

{{ parse_avro('people.colleague.data.event-value.avsc') }}

 -- depends_on: {{ ref('dbt_results') }}