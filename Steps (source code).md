# Step 1: Create BigQuery Dataset and External Connection

# Step 2: Deploy Function Calling (Java Cloud Function)

CREATE OR REPLACE FUNCTION 
  `mdm_gemini.MDM_GEMINI` (latlng STRING) RETURNS STRING
  REMOTE WITH CONNECTION `us.gemini-bq-conn`
  OPTIONS (
    endpoint = 'https://us-central1-YOUR_PROJECT_ID.cloudfunctions.net/gemini-fn-calling', max_batching_rows = 1
  );

### WORKAROUND: 
If you do not have the necessary key or Cloud Function deployed, feel free to jump to the landing table directly by exporting the data from the csv into your new BigQuery dataset mdm_gemini using the following command in the Cloud Shell Terminal:
Remember to download the file CITIBIKE_STATIONS.csv from this repo into your Cloud Shell project folder and navigate into that folder before executing the following command:

bq load --source_format=CSV --skip_leading_rows=1 mdm_gemini.CITIBIKE_STATIONS_test ./CITIBIKE_STATIONS.csv \ name:string,latlng:string,capacity:numeric,num_bikes_available:numeric,num_docks_available:numeric,last_reported:timestamp,full_address_json:json

# Step 3: Create Table and Enrich Address data

If you have used the WORKAROUND approach from the last step, you can skip this step, since you have already created the table there. If NOT, proceed to the running the following DDL in BigQuery SQL Editor:

CREATE TABLE mdm_gemini.CITIBIKE_STATIONS as (
select  name, latitude || ',' || longitude as latlong, capacity, num_bikes_available, num_docks_available,last_reported,
'' as full_address_string 
 from bigquery-public-data.new_york_citibike.citibike_stations) ;

Let’s enrich the address data by invoking the remote function on the latitude and longitude coordinates available in the table. Please note that we will update this only for data reported for the year 2024 and where number of bikes available > 0 and capacity > 100:

update `mdm_gemini.CITIBIKE_STATIONS` 
set full_address_string = `mdm_gemini.MDM_GEMINI`(latlong) 
where EXTRACT(YEAR FROM last_reported) = 2024 and num_bikes_available > 0 and capacity > 100;

Do not skip this step even if you used the WORKAROUND approach in the last step. 
Let’s create a second source of bike station location data for the purpose of this use case. Afterall, MDM is bringing data from multiple sources together and identifying the golden truth.

Run the following DDLs in BigQuery SQL Editor for creating the second source of location data with 2 records in it. Let’s call this table mdm_gemini.CITIBIKE_STATIONS_SOURCE2.

CREATE TABLE mdm_gemini.CITIBIKE_STATIONS_SOURCE2 (name STRING(55), address STRING(1000), embeddings_src ARRAY<FLOAT64>);

insert into mdm_gemini.CITIBIKE_STATIONS_SOURCE2 VALUES ('Location broadway and 29','{ "DOOR_NUMBER": "1593", "STREET_ADDRESS": "Broadway", "AREA": null, "CITY": "New York", "TOWN": null, "COUNTY": "New York County", "STATE": "NY", "COUNTRY": "USA", "ZIPCODE": "10019", "LANDMARK": null}');

insert into mdm_gemini.CITIBIKE_STATIONS_SOURCE2 VALUES ('Allen St & Hester','{ "DOOR_NUMBER": "36", "STREET_ADDRESS": "Allen St", "AREA": null, "CITY": "New York", "TOWN": null, "COUNTY": "New York County", "STATE": "NY", "COUNTRY": "USA", "ZIPCODE": "10002", "LANDMARK": null}');

# Step 4: Generate Embeddings for Address Data

Run the below DDL to create a remote model for Vertex AI text embeddings API:

CREATE OR REPLACE MODEL `mdm_gemini.CITIBIKE_STATIONS_ADDRESS_EMB`
REMOTE WITH CONNECTION `us.gemini-bq-conn`
OPTIONS (ENDPOINT = 'textembedding-gecko@latest');

Now that the remote embeddings model is ready, let’s generate embeddings for the first source and store it in a table. You can store the embeddings result field in the same mdm_gemini.CITIBIKE_STATIONS table as before, but I am choosing to create a new one for clarity:

CREATE TABLE `mdm_gemini.CITIBIKE_STATIONS_SOURCE1` AS (
SELECT *
FROM ML.GENERATE_EMBEDDING(
  MODEL `mdm_gemini.CITIBIKE_STATIONS_ADDRESS_EMB`,
  ( select name, full_address_string as content from `mdm_gemini.CITIBIKE_STATIONS` 
  where full_address_string is not null )
   )
);

Let’s generate embeddings for address data in table CITIBIKE_STATIONS_SOURCE2:

update `mdm_gemini.CITIBIKE_STATIONS_SOURCE2` a set embeddings_src =
(
SELECT  ml_generate_embedding_result
FROM ML.GENERATE_EMBEDDING(
  MODEL `mdm_gemini.CITIBIKE_STATIONS_ADDRESS_EMB`,
  ( select name, address as content from `mdm_gemini.CITIBIKE_STATIONS_SOURCE2` ))
where name = a.name) where name is not null;
This should create embeddings for the second source, note that we have created the embeddings field in the same table CITIBIKE_STATIONS_SOURCE2.

To visualize the embeddings are generated for the source data tables 1 and 2, run the below queries:
select name,address,embeddings_src from `mdm_gemini.CITIBIKE_STATIONS_SOURCE2`;
select name,content,ml_generate_embedding_result from `mdm_gemini.CITIBIKE_STATIONS_SOURCE1`;

Let’s go ahead and perform vector search to identify duplicates.

# Step 5: Vector Search for Flagging Duplicate Addresses

In this step, we will search the address embeddings ml_generate_embedding_result column of the `mdm_gemini.CITIBIKE_STATIONS_SOURCE1` table for the top 2 embeddings that match each row of data in the embeddings_src column of the `mdm_gemini.CITIBIKE_STATIONS_SOURCE2` table:

select query.name name1,base.name name2, 
/* (select address from mdm_gemini.CITIBIKE_STATIONS_SOURCE2 where name = query.name) content1, base.content content2, */
 distance
 from VECTOR_SEARCH(
  TABLE mdm_gemini.CITIBIKE_STATIONS_SOURCE1,
  'ml_generate_embedding_result',
  (SELECT * FROM mdm_gemini.CITIBIKE_STATIONS_SOURCE2),
  'embeddings_src',
  top_k => 2 
) where query.name <> base.name
order by distance desc;


Table that we are querying: mdm_gemini.CITIBIKE_STATIONS_EMBEDDINGS on the field 'ml_generate_embedding_result'
Table that we use as base: mdm_gemini.CITIBIKE_STATIONS_SOURCE2 on the field 'embeddings_src'
top_k: specifies the number of nearest neighbors to return. The default is 10. A negative value is treated as infinity, meaning that all values are counted as neighbors and returned.
distance_type: specifies the type of metric to use to compute the distance between two vectors. Supported distance types are EUCLIDEAN and COSINE. The default is EUCLIDEAN.

Let’s try with distance_type set to COSINE:

select query.name name1,base.name name2, 
/* (select address from mdm_gemini.CITIBIKE_STATIONS_SOURCE2 where name = query.name) content1, base.content content2, */
 distance
 from VECTOR_SEARCH(
  TABLE mdm_gemini.CITIBIKE_STATIONS_SOURCE1,
  'ml_generate_embedding_result',
  (SELECT * FROM mdm_gemini.CITIBIKE_STATIONS_SOURCE2),
  'embeddings_src',
  top_k => 2,distance_type => 'COSINE'
) where query.name <> base.name
order by distance desc;
