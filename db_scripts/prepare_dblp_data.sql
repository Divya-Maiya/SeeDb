-- Contributor: Neha Prakash (nehaprakash@umass.edu)

DROP TABLE IF EXISTS paperauths cascade;
DROP TABLE IF EXISTS papers cascade;
DROP TABLE IF EXISTS venue cascade;
DROP TABLE IF EXISTS authors cascade;

CREATE TABLE authors (
    id integer,
    name character varying(200),
    primary key (id)
);

CREATE TABLE venue (
    id integer,
    name character varying(200) NOT NULL,
    year integer NOT NULL,
    school character varying(200),
    volume character varying(50),
    number character varying(50),
    type integer NOT NULL,
    primary key (id)
);


CREATE TABLE papers (
    id integer,
    name character varying(2048) NOT NULL,
    venue integer,
    pages character varying(50),
    url character varying(512),
    primary key(id),
    foreign key (venue) references venue(id)
);

CREATE TABLE paperauths (
	paperid integer,
	authid integer,
	foreign key (paperid) references papers(id),
	foreign key (authid) references authors(id)
);

