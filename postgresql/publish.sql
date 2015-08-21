create schema publish;

GRANT USAGE ON SCHEMA publish TO editor;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA publish TO editor;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA publish TO editor;

GRANT USAGE ON SCHEMA publish TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA publish TO reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA publish TO reader;

CREATE ROLE subscriber
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
COMMENT ON ROLE subscriber IS 'Read-only access to the publish schema and gis';

GRANT USAGE ON SCHEMA publish TO subscriber;
GRANT SELECT ON ALL TABLES IN SCHEMA publish TO subscriber;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA publish TO subscriber;

GRANT USAGE ON SCHEMA gis TO subscriber;
GRANT SELECT ON ALL TABLES IN SCHEMA gis TO subscriber;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA gis TO subscriber;

CREATE TABLE publish.listing (                                                                
    room_id                        integer NOT NULL                                
   ,survey_id                      integer NOT NULL DEFAULT 999999                 
   ,host_id                        integer NULL                                    
   ,room_type                      varchar(255) NULL                               
   ,country                        varchar(255) NULL                               
   ,city                           varchar(255) NULL                               
   ,borough                        varchar(255) NULL                               
   ,neighborhood                   varchar(255) NULL                               
   ,reviews                        integer NULL                                    
   ,overall_satisfaction           float NULL                                      
   ,accommodates                   integer NULL                                    
   ,bedrooms                       decimal(5,2) NULL                               
   ,bathrooms                      decimal(5,2) NULL                               
   ,price                          float NULL                                      
   ,minstay                        integer NULL                                    
   ,last_modified                  timestamp NULL DEFAULT current_timestamp        
   ,latitude                       numeric(30,6) NULL                              
   ,longitude                      numeric(30,6) NULL                              
   ,location                       geometry                                        
   ,PRIMARY KEY (room_id ,survey_id )                                              
)                                                                                  
;           
SELECT UpdateGeometrySRID('publish', 'listing','location',4326)
;

CREATE TABLE publish.survey (                                                 
    survey_id                      SERIAL                             
   ,survey_date                    date NULL DEFAULT current_date     
   ,survey_description             varchar(255) NULL                  
   ,search_area_id                 integer NULL                       
   ,PRIMARY KEY (survey_id)                                           
)                                                                     
;

CREATE TABLE publish.search_area (
    search_area_id                 SERIAL
   ,name                           varchar(255) NULL DEFAULT 'UNKNOWN'
   ,PRIMARY KEY (search_area_id)
)
;                                                    


