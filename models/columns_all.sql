{{ config(materialized='table') }}

with
{{ synth_column_primary_key(name='column_pkey') }}


{{ synth_column_words(name='column_words_std', language_code="en", distribution="uniform", n=5) }}
{{ synth_column_words(name='column_words_null', language_code="en", distribution="uniform", n=5) }}

{{ synth_column_timestamp(name="column_timestamp_std", min='1938-01-01 00:00:00', max='1994-12-31 23:59:59') }}
{{ synth_column_timestamp(name="column_timestamp_std", min='1938-01-01 00:00:00', max='1994-12-31 23:59:59', null_frac=0.2) }}



{{ synth_table(rows=1000) }}

select * from synth_table
