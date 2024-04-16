# MDM with Gemini
Simplify Master Data Management activities with Gemini 1.0 Pro Vision and Function Calling

In this project, we will demonstrate how Gemini 1.0 Pro simplifies master data management applications like enrichment and deduplication, for the citibike_stations data available in the BigQuery public dataset. For this, we will use 
1. BigQuery public dataset bigquery-public-data.new_york_citibike.
2. Gemini Function Calling (a Java Cloud Function that gets the address information using the reverse Geocoding API for the coordinates available with the citibike_stations data) that we have already created in one of our previous articles. 
3. Vertex AI Embeddings API in and Vector Search in BigQuery to identify duplicates.


# High Level Flow Diagram

This diagram represents the flow of data and steps involved in the implementation. Please note that the owner for the respective step is mentioned in the text underneath.

![image](https://github.com/AbiramiSukumaran/MDMwithGemini/assets/13735898/cc2420f3-0df8-4ff0-aed1-d31d9740ac3c)


# Demo

The steps loosely are as follows:

1. Create a BigQuery dataset for the use case. Create a landing table with data from the public dataset table bigquery-public-data.new_york_citibike.citibike_stations.
2. Make sure the Cloud Function that includes Gemini Function Calling for address standardization is deployed.
3. Store the enriched address data in the landing tables (from 2 sources for demo purpose).
4. Invoke Vertex AI Embeddings API from BigQuery on the address data. 
5. Use BigQuery Vector Search to identify duplicate records.

# Check the Blog for the detailed steps

