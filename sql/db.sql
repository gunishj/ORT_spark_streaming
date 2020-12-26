--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_with_oids = false;

--Setup database
DROP DATABASE IF EXISTS ORT;
CREATE DATABASE ORT;
\c ORT;

\echo 'LOADING database'
--  Sample ORT database for PostgreSQL
--  Created by Gunish Jha
--  DISCLAIMER
--  To the best of our knowledge, this data is fabricated, and
--  it does not correspond to real organization.
--  Any similarity to existing organization structure is purely coincidental.

CREATE TYPE GRADE AS ENUM('G8', 'G9','G10','G11','G12','G13','G14','G15','G16','G17','G18');
drop TABLE public.ORG_HIER_TBL;
CREATE TABLE public.ORG_HIER_TBL(
EMP VARCHAR(10) NOT NULL,
GRADE GRADE NULL,
RPT_ST_DT DATE NOT NULL,
RPT_END_DT DATE NOT NULL,
SUPRVSR VARCHAR(10) NULL,
SUPRVSR_HIER TEXT NULL );

CREATE TABLE REPORT_A (
EMP VARCHAR(10) NOT NULL,
GRADE GRADE NULL,
NUM_OF_REPORTEES INT NOT NULL,
RANK INT NOT NULL,
PRIMARY KEY (EMP)
);

CREATE TABLE REPORT_B (
GRADE GRADE NULL,
AVG_DIR_REPORTEES INT NULL,
AVG_TOTAL_REPORTEES INT NULL,
PRIMARY KEY (GRADE)
);

CREATE TABLE REPORT_C (
EMP VARCHAR(10) NOT NULL,
 GRADE GRADE NOT NULL,
PRIMARY KEY (EMP)	
);

CREATE TABLE REPORT_D (
GRADE GRADE NOT NULL,
RESIGNED INT NULL,
JOINED	INT NULL,
PRIMARY KEY (GRADE)
);

--
-- PostgreSQL database dump complete
--