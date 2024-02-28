
## Data Transformation DBT


### Guide
Run the command ```dbt init``` enter a name of your project `dbt_nyc` 


, after that you should see folder `dbt_nyc` with the following structure:

```shell
.
├── README.md
├── dbt_nyc
│   ├── README.md
│   ├── analyses
│   ├── dbt_project.yml # Configuration paths and materialization
│   ├── profiles.yml #if file not exists you must create file in folder
│   ├── macros # Containing functions to use in models
│   ├── models
│   └── dim_memory.sql
        ....
│   └── yellow_taxi.sql
│   └── schema.yml
│   │    └── schema.yml # For testing purposes, e.g., check null columns 
│   ├── seeds # Containing CSV files for ingesting into the database
│   ├── snapshots # Snapshots to record changes to mutable tables over time (SCD 2)
│   └── tests
├── docker-compose.yaml
├── logs
│   └── dbt.log
``` 
Try running the following commands:
- ```dbt run``` 
- ```dbt test``` 
### Result
  ![](../imgs/dbt.png)
