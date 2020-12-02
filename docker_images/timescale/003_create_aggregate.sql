CREATE OR REPLACE FUNCTION mul_mod (state NUMERIC, var NUMERIC, modulus NUMERIC)
RETURNS NUMERIC
LANGUAGE SQL
AS $$
    SELECT MOD(state*var, modulus*modulus)
$$;


CREATE AGGREGATE phe_sum (NUMERIC, NUMERIC) (
    initcond = '1', -- this is the initial state of type POINT
    stype = NUMERIC, -- this is the type of the state that will be passed between steps
    sfunc = mul_mod
);
