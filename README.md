# supermarket-datapipline 

**Data profiling** - Findings and recommendations of data profiling is found at the folder data_profiling <br>

**Data modeling** - Data modeling has been done, and scripts for data creation are found at the folder data_modeling. Below is the ER diagram:
![image info](./data_modeling/images/ER_diagram.png)

**Ingestion** - Data ingestion and transforming into two dim and one fact table was done using python script. Scripts are available in local_pipeline folder.
![image info](./local_pipeline/images/data_load.png)

**Reporting**
Created three reporting queries with windowing function to answer below questions.

- Top 3 selling categories on each store based on sale amount (If more than one product takes any place, include all of them)

![image info](./local_pipeline/images/result-1.png)
![image info](./local_pipeline/images/plot-1.png)

- For each product line, which store sold the most (If more than one store takes the place, include them as well)

![image info](./local_pipeline/images/result-2.png)
![image info](./local_pipeline/images/plot-2.png)

- Which product sold as last product in each store many times

![image info](./local_pipeline/images/result-3.png)
![image info](./local_pipeline/images/plot-3.png)