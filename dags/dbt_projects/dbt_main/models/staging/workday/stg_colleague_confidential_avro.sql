{{
    config(
        materialized='table'
    )
}}

{{ parse_avro('people.colleagueconfidential.data.event-value.avsc') }}

 -- depends_on: {{ ref('dbt_results') }}