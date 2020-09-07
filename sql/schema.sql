create table readings
(
    id varchar constraint readings_pk primary key,
	raw_xml text not null,
	city varchar,
	timestamp timestamp,
	celsius numeric,
	fahrenheit numeric,
	active_from timestamp,
	active_to timestamp,
	active_flag bool
);
