# dbt coding conventions

## Model Naming
Our models (typically) fit into three main categories: staging, integration, warehouse, base/intermediate. The file and naming structures are as follows:
```
├── dbt_project.yml
└── models
    ├── warehouse
    |   └── core
    |       ├── core.yml
    |       ├── user_dim.sql
    |       └── transaction_fct.sql
    ├── integration
    |       ├── intermediate
    |       |   ├── intermediate.yml
    |       |   ├── int__user__unioned.sql
    |       |   ├── int__user__grouped.sql
    |   ├── int__user.sql
    |   ├── int__transaction.sql
    |   └── integration.yml
    └── staging
        └── source_a
            ├── stg_source_a.yml
            ├── stg_source_a__user.sql
            ├── stg_source_a__transaction.sql
```
- All objects should be singular, such as: `stg_source_a__user`
- Intermediate tables should end with a past tense verb indicating the action performed on the object, such as: `int__user__unioned`
- Warehouses are categorized between fact (immutable, verbs) and dimensions (mutable, nouns) with a suffix that indicates either, such as: `transaction_fct` or `user_dim`
    - Models in the warehouse layer should be prefixed with the warehouse name, such as: `finance_revenue_fct`.
    - Models not in the core warehouse should not be prefixed (e.g. `transaction_fct`)

## Model configuration

- Model-specific attributes (like sort/dist keys) should be specified in the model.
- If a particular configuration applies to all models in a directory, it should be specified in the `dbt_project.yml` file.
- In-model configurations should be specified like this:

```python
{{
  config(
    materialized = 'table',
    sort = 'id',
    dist = 'id'
  )
}}
```
- Warehouses should always be configured as tables
- Other layers should generally prefer using a view or CTE materialization

## dbt conventions
* Only `stg_` models (or `base_` models if your project requires them) should select from `source`s.
* All other models should only select from other models in the same layer or below.
* Prefer creating an integration version of a model, even if the integration model only passes data through from stage models
  via  select *.
* `int__` models can be used to
    * union multiple sources into a conformed shape
    * merge conceptual rows into a single row
* Guidance for when to use staging, integration, fact, dimension and XA models:
![Model Selection](model_selection.png)

## Testing

- Every subdirectory should contain a `schema.yml` file, in which each model in the subdirectory is tested.
- At a minimum, unique and not_null tests should be applied to the primary key of each model.  Utilize `dbt_utils.unique_combination_of_columns` in integration models to enforce uniqueness when multiple sources are integrated in the same row.
- To validate that new development work doesn't break xa model's validated results, we should use regression tests on top of those xa models. In the `analysis/regression_tests` folder, you can add regression tests on top of an xa model to validate specific metric. Once created, you can execute them by running `dbt compile`.

## Naming and field conventions

* Schema, table and column names should be in `snake_case`.
* Use names based on the _business_ terminology, rather than the source terminology.
* Each model should have a primary key.
* identifiers from source systems should be named `<descriptive name>_natural_key` e.g. `user_source_a_natural_key`
* The primary key of an end-user model should be named `<object>_pk`, e.g. `transaction_pk` – this makes it easier to know what `pk` is being referenced in downstream joined models.
* Foreign keys of an end-user model should be named `<referenced_model>_fk`, e.g. `transaction_fk`
* `pk` and `fk` values should be generated using `dbt_util.surrogate_key`.  Avoid looking up `pk`s in a separate query
* For base/staging models, fields should be organized into the following categories and then sorted alphabetically:
    * keys
    * dates and timestamps
    * attributes (columns used to slice data)
    * metrics (measures)
    * meta data field (e.g. insert timestamps, last modified timestamp, etc)
* Timestamp columns should be named `<event>_ts`, e.g. `created_ts`, and should be in UTC. If a different timezone is being used, this should be indicated with a suffix, e.g `created_ts_ct`.
* Booleans should be prefixed with `is_` or `has_`.
* Price/revenue fields should be in decimal currency (e.g. `19.99` for $19.99; many app databases store prices as integers in cents). If non-decimal currency is used, indicate this with suffix, e.g. `price_in_cents`.
* Avoid reserved words as column names
* Consistency is key! Use the same field names across models where possible, e.g. a key to the `load` table should be named `transaction_fk` rather than `purchase_fk`.
    * common fields, such as `name`, should be prefixed with the entity, e.g. `carrier_name`

## CTEs

- All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do
- prefixing CTEs with `s_` can help distinguish a select from a model vs a CTE in the model file.
- CTEs with confusing or notable logic should be commented
- CTEs that are duplicated across models should be pulled out into their own models or macros.
- create a `final` or similar CTE that you select from as your last line of code. This makes it easier to debug code within a model (without having to comment out code!)
- CTEs should be formatted like this:

``` sql
with

s_events as (

    ...

),

-- CTE comments go here
s_filtered_events as (

    ...

)

select * from s_filtered_events
```

## SQL style guide

- Indents should be four spaces (except for predicates, which should line up with the `where` keyword)
- Lines of SQL should be no longer than 80 characters
- Field names and function names should all be lowercase
- The `as` keyword should be used when aliasing a field or table
- Fields should be stated before aggregates / window functions
- Aggregations should be executed as early as possible before joining to another table.
- Ordering and grouping by a column name (eg. group by carrier_name, shipper_name is preferred. Note that if you are grouping by more than a few columns, it may be worth revisiting your model design.
- Prefer `union all` to `union distinct` [*](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#set_operators)
- Avoid table aliases in join conditions (especially initialisms) – it's harder to understand what the table called "c" is compared to "customer".
- If joining two or more tables, _always_ prefix your column names with the table alias. If only selecting from one table, prefixes are not needed.
- Be explicit about your join (i.e. write `inner join` instead of `join`). `left joins` are normally the most useful, `right joins` often indicate that you should change which table you select `from` and which one you `join` to.
- when possible, configure [sqlfluff](https://www.sqlfluff.com) to enforce consistency
- *DO NOT OPTIMIZE FOR A SMALLER NUMBER OF LINES OF CODE. NEWLINES ARE CHEAP, BRAIN TIME IS EXPENSIVE*

### Example SQL
```sql
with

s_data1 as (

    select * from {{ ref('my_data1') }}

),

s_data2 as (

    select * from {{ ref('my_data2') }}

),

some_cte_agg as (

    select
        id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5

    from s_data2
    group by 1

),

final as (

    select [distinct]
        s_data1.field_1,
        s_data1.field_2,
        s_data1.field_3,

        -- use line breaks to visually separate calculations into blocks
        case
            when s_data1.cancellation_date is null
                and s_data1.expiration_date is not null
                then expiration_date
            when s_data1.cancellation_date is null
                then s_data1.start_date + 7
            else s_data1.cancellation_date
        end as cancellation_date,

        some_cte_agg.total_field_4,
        some_cte_agg.max_field_5

    from s_data1
    left join some_cte_agg
        on s_data1.id = some_cte_agg.id
    where s_data1.field_1 = 'abc'
        and (
            s_data1.field_2 = 'def' or
            s_data1.field_2 = 'ghi'
        )
    having count(*) > 1

)

select * from final

```

- Your join should list the "left" table first (i.e. the table you are selecting `from`):
```sql
select
    trips.*,
    drivers.rating as driver_rating,
    riders.rating as rider_rating

from trips
left join users as drivers
    on trips.driver_id = drivers.user_id
left join users as riders
    on trips.rider_id = riders.user_id

```

## YAML style guide

* Indents should be two spaces
* List items should be indented
* Use a new line to separate list items that are dictionaries where appropriate
* Lines of YAML should be no longer than 80 characters.

### Example YAML
```yaml
version: 2

models:
  - name: events
    columns:
      - name: event_id
        description: This is a unique identifier for the event
        tests:
          - unique
          - not_null

      - name: event_time
        description: "When the event occurred in UTC (eg. 2018-01-01 12:00:00)"
        tests:
          - not_null

      - name: user_id
        description: The ID of the user who recorded the event
        tests:
          - not_null
          - relationships:
              to: ref('users')
              field: id
```

## Documentation guide
* stage models and columns should be 100% documented.  All analytics engineers will be pulling data from stage models.  Having clear readily documentation of the meaning of data is critical.
* end-user models should also be 100% documented.  End users need clear definition of data to work with the data
* intermediate models and fields should be documented as needed to clarify any special cases
* use doc blocks to capture documentation that can be shared between models.  This will help maintain consistency with the documentation and cut down on maintenance of shared documentation.  one-off documentation can be inlined.
* documentation coverage is enforced via [dbt-meta-testing](https://github.com/tnightengale/dbt-meta-testing)

## Jinja style guide

* When using Jinja delimiters, use spaces on the inside of your delimiter, like `{{ this }}` instead of `{{this}}`
* Try to strike a balance between readable jinja and readable sql.
    * Favor for jinja template readability
    * don't spend too much time trying to get the generated sql to look nice
* Use newlines to visually indicate logical blocks of Jinja
