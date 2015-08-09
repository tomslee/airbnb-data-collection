create or replace view city_survey
  as select sa.name as city,
    max(s.survey_id) as newer,
    min(s.survey_id) as older
    from survey as s join search_area as sa
      on s.search_area_id = sa.search_area_id
    group by city
    having count(*) > 1
;

create or replace function survey_room ( in integer ) 
returns table ( 
  room_id integer,
  host_id integer,
  room_type varchar(255),
  country varchar(255),
  city varchar(255),
  neighborhood varchar(255),
  address varchar(1023),
  reviews integer,
  overall_satisfaction double precision,
  accommodates integer,
  bedrooms decimal(5,2),
  bathrooms decimal(5,2),
  price double precision,
  deleted integer,
  minstay integer,
  last_modified timestamp,
  latitude numeric(30,6),
  longitude numeric(30,6),
  survey_id integer ) as
$BODY$
  select room_id, host_id, room_type,
    country, city, neighborhood, address, reviews,
    overall_satisfaction, accommodates, bedrooms,
    bathrooms, price, deleted, minstay, last_modified,
    latitude, longitude, survey_id
    from room as r
    where r.survey_id = $1
    and price is not null
    and deleted = 0;
$BODY$ language sql
;

drop function if exists survey_host(int);
create function survey_host( in integer ) 
returns table (
  host_id int,
  rooms bigint,
  multilister smallint,
  review_count bigint,
  addresses bigint,
  rating numeric(4,2),
  income1 double precision,
  income2 double precision) as 
$BODY$
  select 
    host_id,
    count(*) as rooms,
    cast((case when count(*) > 1 then 1 else 0 end) as smallint) as multilister,
    sum(reviews) as review_count,
    count(distinct address) as addresses,
    cast(sum(overall_satisfaction*reviews)/sum(reviews) as numeric(4,2)) as rating,
    sum(reviews*price) as income1,
    sum(reviews*price*minstay) as income2
    from survey_room($1)
    where reviews > 0 and minstay is not null
    group by host_id;
$BODY$ language sql
;

create function add_survey( in varchar(255) ) as
$BODY$
  insert into survey( survey_description, search_area_id )
    select(name || ' (' || current_date || ')') as survey_description,
      search_area_id
      from search_area
      where name = $1;
$BODY$ language sql
;

create function new_room( in old_survey_id integer, in new_survey_id integer ) 
returns table ( "room_id" integer ) as
$BODY$
  select "room_id"
    from 
  (select "room_id" from "survey_room"(new_survey_id) 
  except
  select "room_id" from "survey_room"(old_survey_id)) 
  as "t"
$BODY$ language sql

create function trg_location()
returns trigger
as
$trg_location$
BEGIN
  NEW.location := st_setsrid(
  st_makepoint(NEW.longitude, NEW.latitude), 
  4326
  );
  RETURN NEW;
 END
$trg_location$ language plpgsql;

create trigger trg_location
before insert or update of latitude, longitude
on room
for each row
execute procedure trg_location();