{{ config(materialized='table') }}

with
{{ synth_column_primary_key(name='user_id') }}
{{ synth_column_primary_key(name='anonymous_id') }}
{{ synth_column_firstname(name='first_name') }}
{{ synth_column_lastname(name='last_name') }}
{{ synth_column_email_domain(name='email_domain') }}
{{ synth_column_expression(name='email2', expression="lower(first_name||'.'||last_name||'@'||email_domain)") }}
{{ synth_column_email_address(name='email1') }}
{{ synth_column_distribution(name='popularity',
    distribution=synth_distribution(class='continuous', type='exponential', lambda=0.05)
) }}
{{ synth_table(rows=100) }}

select * from synth_table
