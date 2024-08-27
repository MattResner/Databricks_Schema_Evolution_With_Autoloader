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

**Quarter 1 File**

![image](https://github.com/user-attachments/assets/6fdea9b0-16e7-41bb-85af-cef28ad5d8e9)

**Quarter 3 File**

![image](https://github.com/user-attachments/assets/050e98f8-ce71-4103-a0ea-e43719744ed4)

**The resulting dataset should allow any reader/learner to create a a pipeline to practice:**
1. Schema evolution of COGS (Cost of Goods Sold) and Profit in the first hop or bronze layer,
2. The detection and removal of null rows and duplicate quarterly values on batch and store id,
3. Missing profit values calculated or erronous values replaced for each store, and
4. Aggregation performed to calculate quarterly profit by region.

Optionally I've also envisioned using this dataset to practice enrichment by joining a demensional file with each Trader Joes location with data on Metropolitian Statistical Area, to aggregate on that rather than the individual store location. 

The python notebook is downloadable at [0. Simulating Retail Revenue Data.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/0.%20Simulating%20Retail%20Revenue%20Data.ipynb)

The file source data and subsequently created quarterly revenue files are available at [Source Files for Demo](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/tree/main/Source%20Files%20for%20Demo)

## Creating our Demonstration Environment

In creating our source data the next step in our demonstration is the creation of our database, tables and directories. 

This work is accomplished in [1. (Databricks) Database Configuration.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/1.%20(Databricks)%20Database%20Configuration.ipynb)

When the notebook is run you should have the following directories created in your Databricks File Store:

![image](https://github.com/user-attachments/assets/522ff0b6-a1bc-42fa-a65e-c3932b27a88b)


## Ingesting our Four Quarterly "Batches" with Autoloader and Schema Evolution

After enviroment creation we then will navigate to dbfs:/FileStore/tables/TraderJoesRevenue/Files to drop the first source file "Simulated_TJ_Revenue_03.31.2023.csv" 

**Dropping the first quarterly file**
![image](https://github.com/user-attachments/assets/e8e309ec-0cb0-47fe-9bbf-cb72d509199a)

**Run the Autoloader Bronze Layer (Q1)**

Open and run source code in [2. (Databricks) Bronze Fact Pipeline.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/2.%20(Databricks)%20Bronze%20Fact%20Pipeline.ipynb)

![image](https://github.com/user-attachments/assets/ff7128a4-4985-43dc-a72b-e3e3d03f4ac5)

The resulting data will be loaded into our delta table bronze_tj_fact_revenue which can be queried with the below sql available in [3. (Databricks) Reporting Queries.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/3.%20(Databricks)%20Reporting%20Queries.ipynb)

![bronze sql first run Quarter](https://github.com/user-attachments/assets/aa6848aa-40af-4bd6-a76d-34b309201469)

## Querying to See the Schema Changes

You can then run additional queries in [3. (Databricks) Reporting Queries.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/3.%20(Databricks)%20Reporting%20Queries.ipynb) to see schema metadata as you continue to drop the remaining files for each sequential update to the bronze table schema.

Note that after each sequential file with a schema change (Quarters 2 and 3). The ingestion autoloader step will fail and output the below exceptions. This is intentional. In an production deployment of this function you would want to wrap the autoloader step in try/catch error handling that would notify users with a message if the autoloader step fails and pass along the message indicating which column was added to the schema. 

**COGS Column Added**
![Exception due to addition of COGS Column](https://github.com/user-attachments/assets/10206575-543b-42d1-b90e-91d07be911fd)

**Profit Column Added**
![Rerun after initial failure that adds profit column](https://github.com/user-attachments/assets/84f238df-7d73-40cc-9d1e-070fd3f34328)

Example query of the delta log schema change with metadata
![image](https://github.com/user-attachments/assets/0146de78-fa28-41a3-9cae-41d7bc922d30)

After completion you should have a complete delta log record and all quarters will be present in the data:

All Quarters are present in the data. Note that each quarter doesn't have the same number of records. This is due to initionally created duplicate or null records in Quarters 1-3 to permit you to practice removing and handling these sorts of problems in your own pipelines.
![image](https://github.com/user-attachments/assets/562b5340-2182-4bc8-90f1-1abe38aeadae)

Total Revenue, COGS, and Revenue are all present in the final schema

![image](https://github.com/user-attachments/assets/a3e89099-1cf5-471f-bb6c-3d31aa75e231)


## Cleaning Up our Demo with the Deletion Step

Finally after practicing with our data we should delete the data and schema from storage to keep our enviroment clean. 

Opening and running [9. (Databricks) Cleanup.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/9.%20(Databricks)%20Cleanup.ipynb) will delete all the files, tables, and schema locations created in [1. (Databricks) Database Configuration.ipynb](https://github.com/MattResner/Databricks_Schema_Evolution_With_Autoloader/blob/main/1.%20(Databricks)%20Database%20Configuration.ipynb). 

Returning to the /dbfs/FileStore/tables/TraderJoesRevenue path will confirm that the schema, checkpoints, database tables, and files have been removed

![image](https://github.com/user-attachments/assets/77a3e023-9659-43df-8e02-2c5124d60755)

### Thanks for Reading

If you'd like to reach out to me with questions regarding this project please message me on [Linkedin](https://www.linkedin.com/in/matthewresner/).

Matthew (Rez) Resner




