{{ config(materialized='table') }}

with
{{ synth_column_date(name="column_date_original", min='1990-01-01', max='2030-12-31', distribution='uniform') }}
{{ synth_column_date(name="column_date_null", min='1990-01-01', max='2030-12-31', distribution='uniform', null_frac=0.2) }}
{{ synth_column_email_address(name="column_email_original") }}
{{ synth_column_email_address(name="column_email_null", null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_original", min=0, max=1000000) }}
{{ synth_column_integer(name="column_integer_null", min=0, max=1000000, null_frac=0.2) }}

{{ synth_table(rows=1000) }}

select
    column_date_original,
    column_date_null,
    column_email_original,
    column_email_null,
    column_integer_original,
    column_integer_null
from synth_table
