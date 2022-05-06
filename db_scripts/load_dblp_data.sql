-- Contributor: Neha Prakash (nehaprakash@umass.edu)

SET client_encoding TO 'UTF8';

\COPY authors (id, name) FROM 'authors.csv' WITH (FORMAT CSV, HEADER true);

\COPY venue (id, name, year, school, volume, number, type) FROM 'venue.csv' WITH (FORMAT CSV, HEADER true);

\COPY papers (id, name, venue, pages, url) FROM '.papers.csv' WITH (FORMAT CSV, HEADER true);

\COPY paperauths (paperid, authid) FROM 'paperauths.csv' WITH (FORMAT CSV, HEADER true);
