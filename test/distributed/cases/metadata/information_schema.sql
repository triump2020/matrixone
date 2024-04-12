select information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA,
       information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.TABLE_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.REFERENCED_TABLE_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_NAME,
       information_schema.REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_SCHEMA,
       information_schema.KEY_COLUMN_USAGE.COLUMN_NAME
from information_schema.REFERENTIAL_CONSTRAINTS
         join information_schema.KEY_COLUMN_USAGE
              on (information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA =
                  information_schema.KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA and
                  information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME =
                  information_schema.KEY_COLUMN_USAGE.CONSTRAINT_NAME and
                  information_schema.REFERENTIAL_CONSTRAINTS.TABLE_NAME =
                  information_schema.KEY_COLUMN_USAGE.TABLE_NAME)
where (information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA in ('plat_content') or
       information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA in ('plat_content'))
order by information_schema.KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA asc,
         information_schema.KEY_COLUMN_USAGE.CONSTRAINT_NAME asc,
         information_schema.KEY_COLUMN_USAGE.ORDINAL_POSITION asc;