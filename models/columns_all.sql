{{ config(materialized='table') }}

with
{{ synth_column_primary_key(name='column_pkey') }}
{{ synth_column_unique_key(name='column_ukey_std') }}
{{ synth_column_unique_key(name='column_ukey_null', null_frac=0.2) }}

{{ synth_column_foreign_key(name='column_fkey_std', model_name='stores', column='k_store') }}
{{ synth_column_foreign_key(name='column_fkey_null', model_name='stores', column='k_store', null_frac=0.2) }}




{{ synth_table(rows=1000) }}

select * from synth_table
