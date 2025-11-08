-- migration: create target schema and table
CREATE TABLE IF NOT EXISTS public.customers (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
email TEXT,
signup_date DATE,
score DOUBLE PRECISION
);
