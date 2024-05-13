# Uber/Taxi-Data-Engineering-Project

This project aimed to implement an ETL data pipeline for yellow and green NYC taxi trip records (for fun, we'll call it Uber). I started by building a dimensional data model in fact and dimension format to visualize the goal. I then wrote the code for transforming the data in python, which was deployed as part of an ETL pipeline on Mage AI through a compute instance on google cloud. From here, I loaded the transformed data into Snowflake for analysis with SQL in preparation for being visualized in Looker. The final dashboard could be used to answer such questions as: do customers paying by credit card or cash spend more per ride? Which tips more? Which rate type (standard, pre-negotiated, airport trip, etc.) provides the highest average sale?

The final visualization can be viewed at: https://lookerstudio.google.com/reporting/fad782bf-59ce-44b8-99ba-dc4ada4241ad

# Project Tools Overview

Dimensional Data Diagram
  * Lucidchart

Raw Data
  * Google Cloud Storage

ETL
  * Virtual Machine Instance
  * Mage AI

Analysis
  * Snowflake

Dashboard Visualization
  * Looker

# Dimensional Data Diagram
![dimensional-data-model](https://github.com/walker-at/Uber-Data-Engineering-Project/assets/161479815/a3f8bba5-e65d-4b37-b09e-1fe3d876fb72)


# About the Data
"Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. The data used in the attached datasets were collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers."

For more, refer to: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

For more information the features of the dataset, refer to the Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
