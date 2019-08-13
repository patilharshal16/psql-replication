# psql-parser
Parse data form WAL generated in Postgres db using "test_decoding" plugin provided in postgresql-contrib package.


### Sample OUTPUT is of the form:

```
table public.test_logic_table: INSERT: name[name]:'first tx changes' pk[bigint]:1
```


