-- product_agg - essentially the same as sum, but for multiplication
-- the command `product` is reserved in PostgreSQL, so we used product_agg instead
-- usage example: select product_agg(column_name) from table_name;
create or replace aggregate product_agg(numeric) (
    sfunc = numeric_mul,  -- use PostgreSQL's built-in multiplication function
    stype = numeric,
    initcond = '1'
);
