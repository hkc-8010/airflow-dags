{{
    config(
        materialized='table'
    )
}}

{{ parse_avro('people.orgunit.data.event-value.avsc') }}

 -- depends_on: {{ ref('dbt_results') }}