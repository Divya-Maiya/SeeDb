-- Contributor: Neha Prakash (nehaprakash@umass.edu)
drop table if exists dblp;

create table dblp(
    year text, 
    school text, 
    venue text, 
    author text, 
    pages real, 
    title text, 
    coauthors real, 
    venue_type real
);

-- copy dblp FROM '../data/dblp/cleaned_final_view.data' with (DELIMITER('#'));