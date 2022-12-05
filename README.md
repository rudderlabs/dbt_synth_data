# dbt_synth_data

This is a [`dbt`](https://www.getdbt.com/) package for creating synthetic data. Currently it supports [Snowflake](https://www.snowflake.com/en/) and [Postgres](https://www.postgresql.org/). Other backends may be added eventually.

See `models/*` for examples of usage, further documentation will come soon.

`seeds/*` contains various files which are used to generate realistic random values of various types.

All the magic happens in `macros/*`.



## Architecture
Robert Fehrmann (CTO at Snowflake) has a couple good blog posts about [generating random integers and strings](https://www.snowflake.com/blog/synthetic-data-generation-at-scale-part-1/) or [dates and times](https://www.snowflake.com/blog/synthetic-data-generation-at-scale-part-2/) in Snowflake, which the [base column types](#base-column-types) in this package emulate.

However, creating more realistic synthetic data requires more complex data types, advanced random distributions, seed data, and correlated subqueries or lookups on other tables. Unfortunately, due to how database engines are designed, *subqueries of expressions based on or derived from* a `RANDOM()` value are "optimized" so that <ins>every row has the same value</ins>. Therefore advanced column types in this package use a (slower) multi-step process to generate distinct values for each row:

1. an intermediate column is added to the table containing a `RANDOM()` number
1. an `update` query is run on the table which populates a new column with values based on the `RANDOM()` value from the intermediate column
1. finally, the table is "cleaned up" by removing the intermediate column (and any other temporary columns that were created to build up a more complex value)

These steps are handled using `dbt`'s [post hooks](https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook) feature, which is why you *must* include the following at the bottom of every model you build with this package:
```
{{ config(post_hook=dbt_synth.get_post_hooks())}}
```
Hook queries (and other package data) are stored in the `dbt` [`builtins` object](https://docs.getdbt.com/reference/dbt-jinja-functions/builtins) during parse/run time, as this is one of few dbt objects that persist and are scoped across `macro`s.



## Simple Example
Consider the example model `orders.sql` below:
```sql
-- depends_on: {{ ref('products') }}
{{ config(materialized='table') }}
{{ dbt_synth.table(
    rows = 5000,
    columns = [
        dbt_synth.column_primary_key(name='order_id'),
        dbt_synth.column_foreign_key(name='product_id', table='products', column='product_id'),
        dbt_synth.column_values(name='status', 
            values=["New", "Shipped", "Returned", "Lost"],
            distribution=dbt_synth.distribution_discrete_probabilities(probabilities=[0.2, 0.5, 0.2, 0.1])),
        dbt_synth.column_integer(name='num_ordered',
            distribution=dbt_synth.distribution_discrete_uniform(min=1, max=10)),
    ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
```
The model begins with a [dependency hint to dbt](https://docs.getdbt.com/reference/dbt-jinja-functions/ref#forcing-dependencies) for another model `products`. The model is also materialized as a table, to persist the new data in the database. Next, a new table is created with 5000 rows and several columns:
* `order_id` is the primary key on the table - it wil contain a unique hash value per row
* `product_id` is a foreign key to the `products` table - values in this column will be uniformly-distributed, valid primary keys of the `products` table
* each order has a `status` with several possible values, whose prevalence/likelihoods are given by a discrete probability distribution
* `num_ordered` is the count of how many of the product were ordered, a uniformly-distributed integer from 1-10

Finally, the model adds the post hooks required to finish building and clean up the new table.



## Distributions
This package provides the following distributions:

### Continuous Distributions
<details>
<summary><code>uniform</code></summary>

Generates [uniformly-distributed](https://en.wikipedia.org/wiki/Continuous_uniform_distribution) real numbers.
```python
    dbt_synth.distribution_continuous_uniform(min=0.6, max=7.9, precision=4)
```
Default `min` is `0.0`. Default `max` is `1.0`. `min` and `max` are inclusive. Default `precision` is full precision.
</details>

<details>
<summary><code>normal</code></summary>

Generates [normally-distributed (Gaussian)](https://en.wikipedia.org/wiki/Normal_distribution) real numbers.
```python
    dbt_synth.distribution_continuous_unormal(mean=5, stddev=0.5, precision=2)
```
Default `mean` is `0.0`, default `stddev` is `1.0`. Default `precision` is full precision.
</details>


### Discrete Distributions
<details>
<summary><code>uniform</code></summary>

Generates [uniformly-distributed](https://en.wikipedia.org/wiki/Discrete_uniform_distribution) integers.
```python
    dbt_synth.distribution_discrete_uniform(min=6, max=55)
```
Default `min` is `0`, default `max` is `1`. `min` and `max` are inclusive.
</details>

<details>
<summary><code>normal</code></summary>

Generates [normally-distributed (Gaussian)](https://en.wikipedia.org/wiki/Normal_distribution) integers.
```python
    dbt_synth.distribution_discrete_normal(mean=5, stddev=0.5)
```
Default `mean` is `0`, default `stddev` is `1`.
</details>

<details>
<summary><code>bernoulli</code></summary>

Generates integers (`0` and `1`) according to a [Bernoulli distribution](https://en.wikipedia.org/wiki/Bernoulli_distribution).
```python
    dbt_synth.distribution_discrete_bernoulli(p=0.3)
```
Default `p` is `0.5`.
</details>

<details>
<summary><code>binomial</code></summary>

Generates integers according to a [Binomial distribution](https://en.wikipedia.org/wiki/Binomial_distribution).
```python
    dbt_synth.distribution_discrete_binomial(n=100, p=0.3)
```
Default `n` is `10`, default `p` is `0.5`.

Note that the implementation is approximate, based on a normal distribution (see [here](https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation)). For very "wide" binomial distributions (large `n`) or "skew" binomial distributions (extreme `p`), normally-distributed values may be `< 0` or `> n`, which is impossible in a binomial distribution. These long-tail values are rare, so, while not completely correct, we use
* `abs()` to shift those `< 0`
* `mod(..., n+1)` to shift those `> n`

This may artificially increase small values slightly.
</details>

<details>
<summary><code>probabilities</code></summary>

Generates integers (`0` and `1`) according to a user-defined probability set.
```python
    dbt_synth.distribution_discrete_probabilities(probabilities={"1":0.15, "5":0.5, "8": 0.35})
```
`probabilities` is required and has no default. It may be a list (array), in which case the indices are the integer values generated, or a dictionary (key-value) structure, in which case the keys are the integers generated. `probabilities` must sum to `1.0`.
</details>

More advanced distributions can be constructed from combinations of the above. For example, we can make a *bi-modal distribution* as follows:
```python
{{ dbt_synth.table(
  rows = 100000,
  columns = [
    dbt_synth.distribution(name='normal_0',  class='continuous', type='normal', mean=5.0, stddev=1.0),
    dbt_synth.distribution(name='normal_1',  class='continuous', type='normal', mean=8.0, stddev=1.0),
    dbt_synth.distribution(name='which_one', class='discrete',   type='bernoulli'),
    dbt_synth.column_expression(name='continuous_bimodal',
        expression='(case when which_one=0 then normal_0 else normal_1 end)'),
  ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column which_one") or "" }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column normal_0") or "" }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column normal_1") or "" }}
```


## Column types
This package provides the following column types:


### Base column types
Basic column types, which are quite performant.

<details>
<summary><code>boolean</code></summary>

Generates boolean values.
```python
    dbt_synth.column_boolean(name='is_complete', pct_true=0.2),
```
</details>

<details>
<summary><code>integer</code></summary>

Generates integer values.
```python
    dbt_synth.column_integer(name='event_year', min=2000, max=2020, distribution='uniform'),
```
</details>

<details>
<summary><code>integer sequence</code></summary>

Generates an integer sequence (value is incremented at each row).
```python
    dbt_synth.column_integer_sequence(name='day_of_year', step=1, start=1),
```
</details>

<details>
<summary><code>numeric</code></summary>

Generates numeric values.
```python
    dbt_synth.column_numeric(name='price', min=1.99, max=999.99, precision=2),
```
</details>

<details>
<summary><code>string</code></summary>

Generates random strings.
```python
    dbt_synth.column_string(name='password', min_length=10, max_length=20),
```
String characters will include `A-Z`, `a-z`, and `0-9`.
</details>

<details>
<summary><code>date</code></summary>

Generates date values.
```python
    dbt_synth.column_date(name='birth_date', min='1938-01-01', max='1994-12-31'),
```
</details>

<details>
<summary><code>date sequence</code></summary>

Generates a date sequence.
```python
    dbt_synth.column_date_sequence(name='calendar_date', start_date='2020-08-10', step=3),
```
</details>

<details>
<summary><code>primary key</code></summary>

Generates a primary key column.
```python
    dbt_synth.column_primary_key(name='product_id'),
```
</details>

<details>
<summary><code>value</code></summary>

Generates a (single, static) value for every row.
```python
    dbt_synth.column_value(name='is_registered', value='Yes'),
```
</details>

<details>
<summary><code>values</code></summary>

Generates values from a list of possible values, with optional weighting.
```python
    dbt_synth.column_values(name='academic_subject', values=['Mathematics', 'Science', 'English Language Arts', 'Social Studies'], weights=[0.2, 0.3, 0.15, 0.35]),
```
</details>

<details>
<summary><code>mapping</code></summary>

Generates values by mapping from an existing column or expresion to values in a dictionary.
```python
    dbt_synth.column_mapping(name='day_type', expression='is_school_day', mapping=({ true:'Instructional day', false:'Non-instructional day' })),
```
</details>

<details>
<summary><code>expression</code></summary>

Generates values based on an expression (which may refer to other columns, or invoke SQL functions).
```python
    dbt_synth.column_expression(name='week_of_calendar_year', expression="DATE_PART('week', calendar_date)::int", type='int'),
```
</details>


### Statistical column types
Statistical column types can be used to make advanced statistical relationships between tables and columns.

<details>
<summary><code>correlation</code></summary>

Generates two or more columns with correlated values.
```python
    {% set birthyear_grade_correlations = ({
        "randseed": dbt_synth.get_randseed(),
        "columns": {
            "birth_year": [ 2010, 2009, 2008, 2007, 2006, 2005, 2004 ],
            "grade": [ 'Eighth grade', 'Ninth grade', 'Tenth grade', 'Eleventh grade', 'Twelfth grade' ]
        },
        "probabilities": [
            [ 0.02, 0.00, 0.00, 0.00, 0.00 ],
            [ 0.15, 0.02, 0.00, 0.00, 0.00 ],
            [ 0.03, 0.15, 0.02, 0.00, 0.00 ],
            [ 0.00, 0.03, 0.15, 0.02, 0.00 ],
            [ 0.00, 0.00, 0.03, 0.15, 0.02 ],
            [ 0.00, 0.00, 0.00, 0.03, 0.15 ],
            [ 0.00, 0.00, 0.00, 0.00, 0.03 ]
        ]
        })
    %}
    ...
    {{ dbt_synth.table(
        rows = var('num_students'),
        columns = [
            dbt_synth.column_primary_key(name='k_student'),
            dbt_synth.column_correlation(data=birthyear_grade_correlations, column='birth_year'),
            dbt_synth.column_correlation(data=birthyear_grade_correlations, column='grade'),
            ...
        ]
    ) }}
    {{ config(post_hook=dbt_synth.get_post_hooks())}}
```
To created correlated columns, you must specify a `data` object representing the correlation, which contains
* `columns` is a list of column names and possible values.
* `probabilities` is a hypercube, with dimension equal to the number of `columns`, the elements of which sum to `1.0`, indicating the probability of each possible combination of values for the `columns`. The outermost elements of the `probabilities` hypercube corresond to the values of the first column; the innermost elements of the hypercube correspond to the values of the last column. Each dimension of the hypercube must have the same size as the number of values for its corresponding column.

Constructing a `probabilities` hypercube of dimension more than two or three can be difficult &ndash; we recommend adding (temporary) comments and using indentation to keep track of columns, values, and dimensions.
</details>


### Reference column types
Column types which reference values in another table.

<details>
<summary><code>foreign key</code></summary>

Generates values that are a primary key of another table.
```python
    dbt_synth.column_foreign_key(name='product_id', table='products', column='id'),
```
</details>

<details>
<summary><code>lookup</code></summary>

Generates values based on looking up values from one column in another table..
```python
    dbt_synth.column_lookup(name='gender', value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']),
```
(`funcs` is an optional array of SQL functions to wrap the `from_col` value in prior to doing the lookup.)
</details>

<details>
<summary><code>select</code></summary>

Generates values by selecting them from another table, optionally weighted using a specified column of the other table.
```python
    dbt_synth.column_select(
            name='random_ajective'',
            value_col="word",
            lookup_table="synth_words",
            distribution="weighted",
            weight_col="prevalence",
            filter="types like '%adjective%'",
            funcs=["INITCAP"]
        )
```
The above will generate randomly-chosen adjectives (based on the specified `filter`), weighted by prevalence.
</details>


### Data column types
Data column types use real-world data which is maintained in the `seeds/` directory. Some effort has been made to make these data sets
* **Generalized**, rather than specific to a particular country, region, language, etc. For example, the *words* dictionary contains common words from many common languages, not just English.
* **Statistically rich**, with associated metadata which makes the data more useful by capturing various distributions embedded in the data. For example, the *countries* list includes the (approximate) population and land area of each country, which facilitates generating country lists weighted according to these features. Likewise, the *cities* list has the latitude and longitude coordinates for each city, which facilitates generating fairly realistic coordinates for synthetic addresses.

Data column types may all specify a `distribution="weighted"` and `weight_col="population"` (or similar) to skew value distributions. They may also specify `filter`, which is a SQL `where` expression narrowing down the pool of data values that will be used. Finally, they may specify a `filter_expressions` dictionary which allows dynamic filtering based on expressions which can involve row values from other columns. If, for example, we are creating a country column and pass `filter_expressions` as
```json
{
    "country_name": "INITCAP(my_country_col)",
    "geo_region_code": "my_geo_region_col"
}
```
then a `WHERE` clause like this will result:
```sql
synth_countries.country_name=INITCAP(my_country_col)
AND synth_countries.geo_region_code=my_geo_region_col
```
(`filter_expressions` and `filter` - if any - are combined via logical `AND`.)

<details>
<summary><code>city</code></summary>

Generates a city, selected from the `synth_cities` seed table.
```python
    dbt_synth.column_city(name='city', distribution="weighted", weight_col="population", filter="timezone like 'Europe/%'"),
```
</details>

<details>
<summary><code>country</code></summary>

Generates a country, selected from the `synth_countries` seed table.
```python
    dbt_synth.column_country(name='country', distribution="weighted", weight_col="population", filter="continent='Europe'"),
```
</details>

<details>
<summary><code>geo region</code></summary>

Generates a geo region (state, province, or territory), selected from the `synth_geo_regions` seed table.
```python
    dbt_synth.column_geo_region(name='geo_region', distribution="weighted", weight_col="population", filter="country='United States'"),
```
</details>

<details>
<summary><code>first name</code></summary>

Generates a first name, selected from the `synth_firstnames` seed table.
```python
    dbt_synth.column_firstname(name='first_name', filter="gender='Male'"),
```
</details>

<details>
<summary><code>last name</code></summary>

Generates a last name, selected from the `synth_lastnames` seed table.
```python
    dbt_synth.column_lastname(name='last_name'),
```
</details>

<details>
<summary><code>word</code></summary>

Generates a single word, selected from the `synth_words` seed table.
```python
    dbt_synth.column_word(name='random_word', language_code="en", distribution="weighted", pos=["NOUN", "VERB"]),
```
The above generates a randomly-selected English noun or verb, weighted according to frequency.

Rather than `language_code` you may specify `language` (such as `language="English"`), but a language *must* be specified with one of these parameters. See [Words (Datasets)](#words) for a list of supported languages and parts of speech.
</details>

<details>
<summary><code>words</code></summary>

Generates several words, selected from the `synth_words` seed table.
```python
    dbt_synth.column_words(name='random_phrase', language_code="en", distribution="uniform", n=5, funcs=["INITCAP"]),
```
The above generates a random string of five words, uniformly districbuted, with the first letter of each word capitalized.

Alternatively, you can generate words using format strings, for example
```python
    dbt_synth.column_words(name='course_title', language_code="en", distribution="uniform", format_strings=[
        "{ADV} learning for {ADJ} {NOUN}s",
        "{ADV} {VERB} {NOUN} course"
        ], funcs=["INITCAP"]),
```
This will generate sets of words according to one of the format strings you specify.

Note that this data type is constructed by separately generating a single word `n` times (or, for `format_string`s, the set union of all word instances from any `format_string`) and then concatenating them together, which can be slow if `n` is large (or you have many tokens in your `format_string`s).

Rather than `language_code` you may specify `language` (such as `language="English"`), but a language *must* be specified with one of these parameters. See [Words (Data Sets)](#words) for a list of supported languages and parts of speech.
</details>

<details>
<summary><code>language</code></summary>

Generates a spoken language (name or 2- or 3-letter code), selected from the `synth_languages` seed table.
```python
    dbt_synth.column_languages(name='random_lang', type="name", distribution="weighted"),
```
The optional `type` (which defaults to `name`) can take values `name` (the full English name of the language, e.g. *Spanish*), `code2` (the ISO 693-2 two-letter code for the langage, e.g. `es`), or `code3` (the ISO 693-3 three-letter code for the language, e.g. `spa`).
</details>


### Composite column types
Composite column types put together several other column types into a more complex data type.

<details>
<summary><code>address</code></summary>

Generates an address, based on `city`, `geo region`, `country`, `words`, and other values.

Creating a column `myaddress` using this macro will also create intermediate columns `myaddress__street_address`, `myaddress__city`, `myaddress__geo_region`, and `myaddress__postal_code` (or whatever `parts` you specify). You can then `add_update_hook()`s that reference these intermediate columns if you'd like. For example:
```python
{{ dbt_synth.table(
    rows = 100,
    columns = [
        dbt_synth.column_primary_key(name='k_person'),
        dbt_synth.column_firstname(name='first_name'),
        dbt_synth.column_lastname(name='last_name'),
        dbt_synth.column_address(name='home_address', countries=['United States'],
            parts=['street_address', 'city', 'geo_region', 'country', 'postal_code']),
        dbt_synth.column_expression(name='home_address_street', expression="home_address__street_address"),
        dbt_synth.column_expression(name='home_address_city', expression="home_address__city"),
        dbt_synth.column_expression(name='home_address_geo_region', expression="home_address__geo_region"),
        dbt_synth.column_expression(name='home_address_country', expression="home_address__country"),
        dbt_synth.column_expression(name='home_address_postal_code', expression="home_address__postal_code"),
    ]
) }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column home_address") or "" }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
```

Alternatively, you may use something like

```python
{{ dbt_synth.table(
    rows = 100,
    columns = [
        dbt_synth.column_primary_key(name='k_person'),
        dbt_synth.column_firstname(name='first_name'),
        dbt_synth.column_lastname(name='last_name'),
        dbt_synth.column_address(name='home_address_street', countries=['United States'], parts=['street_address']),
        dbt_synth.column_address(name='home_address_city', countries=['United States'], parts=['city']),
        dbt_synth.column_address(name='home_address_geo_region', countries=['United States'], parts=['geo_region']),
        dbt_synth.column_address(name='home_address_country', countries=['United States'], parts=['country']),
        dbt_synth.column_address(name='home_address_postal_code', countries=['United States'], parts=['postal_code']),
    ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
```
</details>


## Advanced Usage
Occasionally you may want to build up a more complex column's values from several simpler ones. This is easily done with an expression column, for example
```python
{{ config(materialized='table') }}
{{ dbt_synth.table(
    rows = 100,
    columns = [
        dbt_synth.column_primary_key(name='k_person'),
        dbt_synth.column_firstname(name='first_name'),
        dbt_synth.column_lastname(name='last_name'),
        dbt_synth.column_expression(name='full_name', expression="first_name || ' ' || last_name"),
    ]
) }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column first_name") or "" }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column last_name") or "" }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
```
Note that you may want to "clean up" by dropping some of your intermediate columns, as shown with the `add_cleanup_hook()` calls in the example above.


## Datasets

### Words
The word list in `seeds/synth_words.csv` contains 70k words &ndash; the top 5k most common words from each of the following 14 languages:
* Bulgarian (`bg`)
* Czech (`cs`)
* Danish (`da`)
* Dutch (`nl`)
* English (`en`)
* Finnish (`fi`)
* French (`fr`)
* German (`de`)
* Hungarian (`hu`)
* Indonesian (`id`)
* Italian (`it`)
* Portuguese (`pt`)
* Slovenian (`sv`)
* Spanish (`es`)

With each word is associated a **frequency**, which is a value between 0 and 1 representing the frequency with which the word appears in common usage of the language, and a **part of speech** for the word, which is one of:
* ADJ: adjective
* ADP: adposition
* ADV: adverb
* AUX: auxiliary verb
* CONJ: coordinating conjunction
* DET: determiner
* INTJ: interjection
* NOUN: noun
* NUM: numeral
* PART: particle
* PRON: pronoun
* PROPN: proper noun
* PUNCT: punctuation
* SCONJ: subordinating conjunction
* SYM: symbol
* VERB: verb
* X: other

Some words may functionally belong to multiple parts of speech; this dataset uses only the single most common.

The dataset is constructed based on word lists and frequencies from [`wordfreq`](https://github.com/rspeer/wordfreq) and part-of-speech tagging from [`polyglot`](https://polyglot.readthedocs.io/en/latest/POS.html). Language availability is based on the set intersection of the languages supported by these two libraries.

### Languages
The language list in `seeds/synth_languages.csv` contains 222 commonly-spoken (living) languages, with, for each, the ISO 693-2 and ISO 693-3 language codes, the approximate number of speakers, and a list of countries in which the language is predominantly spoken. Country names are consistent with those in the countries dataset at `seeds/synth_countries.csv`.

The dataset is assembled primarily from Wikipedia, including [this list of official languages by country](https://en.wikipedia.org/wiki/List_of_official_languages_by_country_and_territory), and the specific pages for each individual language.


## Performance
In Snowflake, using a single Xsmall warehouse:

* Creating `models/dim_student.sql` with 100M rows takes 16 minutes

* Creating 100K `dim_student`s takes 20 secs; 100K `dim_guardian`s takes 30 secs; 200K `fct_family_relationship`s takes around 14 mins.

In Postgres, using an AWS RDS small instance:

* Creating `models/dim_student.sql` with 10M rows didn't finish in 3 hours...

* Creating 100K `dim_student`s takes 43 secs; 100K `dim_guardian`s takes 30 secs; 200K `fct_family_relationship`s takes around 76 minutes.


## Todo
- [ ] fix `distribution_discrete_probabilities()` to not use subquery (may need to post-hook `update`)
- [ ] implement other [distributions](#distributions)... Poisson, Exponential, Gamma, Power law/Pareto, Multinomial?
- [ ] implement methods for combining (adding, multiplying, etc.) distributions
- [ ] update various column types to use new distribution macros
- [ ] document distributions
- [ ] flesh out more seeds, data columns, and composite columns