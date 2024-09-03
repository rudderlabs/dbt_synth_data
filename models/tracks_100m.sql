{{ config(materialized='table') }}

with
{{ synth_column_primary_key(name='id') }}
{{ synth_column_timestamp(name="timestamp2", min='1940-01-01 00:00:00', max='2024-12-31 23:59:59', null_frac=0.2) }}
{{ synth_column_email_address(name="user_email", distribution="weighted", null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_0", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_0", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_1", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_1", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_2", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_2", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_3", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_3", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_4", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_4", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_5", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_5", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_6", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_6", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_7", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_7", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_8", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_8", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_9", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_9", min_length=10, max_length=50, null_frac=0.2) }}

{{ synth_column_integer(name="column_integer_10", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_10", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_11", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_11", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_12", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_12", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_13", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_13", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_14", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_14", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_15", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_15", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_16", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_16", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_17", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_17", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_18", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_18", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_19", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_19", min_length=10, max_length=50, null_frac=0.2) }}

{{ synth_column_integer(name="column_integer_20", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_20", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_21", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_21", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_22", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_22", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_23", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_23", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_24", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_24", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_25", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_25", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_26", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_26", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_27", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_27", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_28", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_28", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_29", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_29", min_length=10, max_length=50, null_frac=0.2) }}

{{ synth_column_integer(name="column_integer_30", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_30", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_31", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_31", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_32", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_32", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_33", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_33", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_34", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_34", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_35", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_35", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_36", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_36", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_37", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_37", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_38", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_38", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_39", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_39", min_length=10, max_length=50, null_frac=0.2) }}

{{ synth_column_integer(name="column_integer_40", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_40", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_41", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_41", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_42", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_42", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_43", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_43", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_44", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_44", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_45", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_45", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_46", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_46", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_47", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_47", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_48", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_48", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_49", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_49", min_length=10, max_length=50, null_frac=0.2) }}

{{ synth_column_integer(name="column_integer_50", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_50", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_51", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_51", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_52", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_52", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_53", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_53", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_54", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_54", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_55", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_55", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_56", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_56", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_57", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_57", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_58", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_58", min_length=10, max_length=50, null_frac=0.2) }}
{{ synth_column_integer(name="column_integer_59", min=0, max=1000, null_frac=0.2) }}
{{ synth_column_string(name="column_string_59", min_length=10, max_length=50, null_frac=0.2) }}

{{ synth_table(rows=100e6) }}

select * from synth_table


