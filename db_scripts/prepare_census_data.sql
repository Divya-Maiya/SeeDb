-- Contributor: Divya Maiya (dmaiya@umass.edu)

drop table IF EXISTS census;

create table census(
	age INTEGER,
	workclass CHAR(50),
	fnlwgt INTEGER,
	education CHAR(50),
	education_num INTEGER,
	marital_status CHAR(50),
	occupation CHAR(50),
	relationship CHAR(50),
	race CHAR(50),
	sex CHAR(20),
	capital_gain INTEGER,
	capital_loss INTEGER,
	hours_per_week INTEGER,
	native_country CHAR(50),
	salary CHAR(50)
);

-- COPY Census FROM '../data/census/adult.data' with (DELIMITER(','));