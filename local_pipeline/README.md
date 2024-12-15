## Data ingestion and reporting 

### Data Ingestion

Pythin script - etl_ingestion_script.py

Kaggle API has been used to dowdnload the data into local

![image info](./images/kaggle_api.png)

Downloaded data has been transformed into product and branch dimension, and sale fact tables.

![image info](./images/transformation.png)


Data has been ingested into the DB

![image info](./images/load.png)


### Reporting

 SQL Script - reporting_query.sql <br>
 Plot script - Plot_report.ipynb <br> 
Created three reporting queries with windowing function to answer below questions.

- Top 3 selling categories on each store based on sale amount (If more than one product takes any place, include all of them)

![image info](./images/result-1.png)
![image info](./images/plot-1.png)

- For each product line, which store sold the most (If more than one store takes the place, include them as well)

![image info](./images/result-2.png)
![image info](./images/plot-2.png)

- Which product sold as last product in each store many times

![image info](./images/result-3.png)
![image info](./images/plot-3.png)