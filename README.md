# Databricks Schema Evolution with Autoloader

### A Simple Illustration of How to Ingest Data from Source Files (CSV) with Added Columns

In my current data engineering role, one of our most frequent and persistant problems is dealing with changes in the structure of data as provided by our upstream providers. 

**For this project I aim to address two challenges that I see frequently in the insurance industry:**

1. A lack of upstream control of source file schema.
2. The often siloed relationships between our Data Engineering team, internal business stakeholders, and the external providers of the source data files.

Though my requirements are driven by my experience within insurance, this demo may also be applicable to other industries with batch source file ingestion of CSVs, upstream stakeholders with which you have have little leverage, and files that frequently have errors or undocumented changes.

### My Demo Requirements

1. The solution must not permantly fail ingestion when source file schemas change (Schema Evolution).
2. Records of schema change must be documented and enable initial notification of schema change as well as subsequent reporting on the changes that occured. 

## Simulating Data for Trader Joes Store Revenue

For this project's data. I opted to create a dataset using a python notebook and some publically available data from [data.world](https://data.world/hdiede/trader-joes/workspace/file?filename=Trader_Joes_USA.csv) to create quarterly total revenue values for each Trader Joes store. I then further transformed the data via excel to manually separate the data by quarter and create new columns to violate strict schema requirements in quarters 2-3. 

![image](https://github.com/user-attachments/assets/6fdea9b0-16e7-41bb-85af-cef28ad5d8e9)



The resulting dataset should allow any reader/learner to create a a pipeline to practice:
1. Schema evolution of COGS (Cost of Goods Sold) and Profit in the first hop or bronze layer,
2. The detection and removal of null rows and duplicate quarterly values on batch and store id,
3. Missing profit values calculated or erronous values replaced for each store, and
4. Aggregation performed to calculate quarterly profit by region.

Optionally I've also envisioned using this dataset to practice enrichment by joining a demensional file with each Trader Joes location with data on Metropolitian Statistical Area, to aggregate on that rather than the individual store location. 

The python notebook is downloadable at [0. Simulating Retail Revenue Data.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/0.%20Simulating%20Retail%20Revenue%20Data.ipynb)

The file source data and subsequently created quarterly revenue files are available at [Source Files for Demo](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/tree/main/Source%20Files%20for%20Demo)

## Creating our Demonstration Environment

The first step in our demonstration is the creation of our database, tables and directories. These steps are accomplished in 

## Ingesting our Four Quarterly "Batches" with Autoloader and Schema Evolution

## Querying to See the Schema Changes

## Cleaning up our demo with
