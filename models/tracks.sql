-- depends_on: {{ ref('fk_users') }}
{{ config(materialized='table') }}

with
{{ synth_column_primary_key(name='id') }}
{#{{ synth_column_string(name='channel') }}#}
{#{{ synth_column_firstname(name='first_name') }}#}
{#{{ synth_column_lastname(name='last_name') }}#}
{#{{ synth_column_email_domain(name='email_domain') }}#}
{#{{ synth_column_address(name='address') }}#}
{{ synth_column_email_address(name='email_address') }}
{{ synth_column_select(name='user',
    model_name="fk_users", value_cols="user_id", distribution="weighted", weight_col="popularity"
) }}
{#{{ synth_column_expression(name='full_name', expression="first_name || ' ' || last_name") }}#}
{#{{ synth_column_expression(name='sort_name', expression="last_name || ', ' || first_name") }}#}
{#{{ synth_column_date(name="birth_date", min='1938-01-01', max='1994-12-31') }}#}
{#{{ synth_column_address(name='shipping_address', countries=['United States'], parts=['street_address', 'city', 'geo_region_abbr', 'postal_code']) }}#}
{#{{ synth_column_phone_number(name='phone_number') }}#}
{#{{ synth_column_timestamp(name='created_on', min='2022-01-01', max='2024-12-31') }}#}
{#{{ synth_column_address(name='email_address', parts=['street_address', 'city', 'geo_region_abbr', 'postal_code']) }}#}
{{ synth_table(rows=1000) }}

select * from synth_table
