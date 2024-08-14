{{ config(materialized='table') }}

with
{{ synth_column_select(name='column_select_original',
    model_name="synth_words",
    value_cols="word",
    distribution="weighted",
    weight_col="frequency",
    filter="part_of_speech like '%ADJ%'"
) }}

{{ synth_column_select(name='column_select_weighted_null',
    model_name="synth_words",
    value_cols="word",
    distribution="uniform",
    weight_col="frequency",
    filter="part_of_speech like '%ADJ%'",
    null_frac=0.2
) }}

{{ synth_column_foreign_key(name='column_fkey_std', model_name='stores', column='k_store') }}
{{ synth_column_foreign_key(name='column_fkey_null', model_name='stores', column='k_store', null_frac=0.2) }}




{{ synth_table(rows=1000) }}

select * from synth_table
