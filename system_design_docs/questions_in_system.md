## Questions asked during System Design Discussion

1. customer table and transactional table, if transactional data has been arrived in transaction table but customer data for the same is not arrived yet and both are having SCD relationship how can we handle this scenario ?
    - We can handle this scenario with three techniques.
    - Each data platform have multiple entities and each entity will have relationships.
    - So let's say the data for a particular date is arrived for customer table, it got loaded into the customer table date partition.
    - And the data for the same date is not arrived for order item entity.
    - So if the pipeline runs upto the consumption layer, then the queries in consumption layer might fail.
    - Because the pipeline always runs on the partitions. At a certain point of time in consumption layer the failure will occur for sure.
    - So to prevent these type of failures we can use three ways.
      - SLA Dependency Trigger method:
        - In this method we put a data availability check at the first step of the pipeline.
        - This will check that all required files for the given date partition is available or not ?
        - After checking the existence of all files the entire pipeline will get triggered otherwise not.
        - This will prevent the problem, but this solution comes with other type of problems.
        - Let's say files for all the entities are available except one, in that case entire workflow will get delayed.
        - Let's say customer file don't get arrived at time which is very small file in size. Except that all the files are arrived.
        - Here the pipeline for entities like orders and delivery takes 1-2 hours in execution.
        - Because of un availability of very small file, entire workflow execution got delayed by 1-2 hours. This might be the bottleneck of the system.
      - Incremental Load:
        - This solution is better as compare to above.
        - In this solution we remove the data availability check from the beginning and put it before loading data to consumption layer.
        - We incrementally load each entity.
        - Once the data is available for a particular file in source system the workflow will trigger for that entity.
        - Pipeline will the data load for all the entities upto clean layer, it holds before consumption layer.
        - We put the data availability check before the consumption layer. It will hold and wait until data for all the entities are available.
        - Once the data availability check completes, we continue with the consumption layer.
      - Change Data Capture:
        - In this solution we can use tools like greate expectations.
2. Explain Surrogate Keys and why do we need them:
   - SCD Type 2 has three main components:
     - Surrogate key, Effective Start and End Date, Is Current/Active flag
     - Surrogate keys are required in clean layer and consumption layer.
     - In the clean layer, the surrogate key works as a primary key with no business logic. It is system-generated and independent of the business data.
     - In the consumption layer, we create a hash key based on all the relevant attributes from the clean table. This hash key serves as the surrogate key for the dimension.
     - While the clean layer surrogate key is typically auto-incremented, the consumption layer hash key is deterministically generated based on business attributes.
     - In the fact table, we use the hash key from the consumption layer dimension tables as foreign keys.
     - For historical data tracking, we can query the fact table to get the hash key and then find the corresponding record in the dimension table, allowing us to see the version of the dimension that was active when the fact was recorded.
   - The primary reasons we need surrogate keys are:
     1. Business keys may change over time
     2. They provide a stable identifier across multiple versions of the same entity
     3. They improve join performance compared to compound natural keys
     4. They abstract the physical implementation from the business meaning
     5. They enable proper SCD Type 2 tracking by giving each version its own unique identifier

3. Why do we need three layers (Bronze, Silver, Gold) in Data Warehouse ? If gold layer is business ready then what is the difference between Gold and Silver ? Why do we need three layer in our data warehouse architecture ? Why not 2 ?
    - Importance of all the layers.
    - Stage: 
      - It contains exact copy of the data loaded from different data sources, No schema which means all the fields are loaded in string datatype.
      - The purpose of this layer is because if there is a failure or data loss in the further layers then we can ingest the data again from this layer rather than asking business to provide the data again.
      - We can use this data for audit purpose.
    - Clean:
      - In this layer we will be performing data quality checks.
      - Which means we will check that input data from stage layer has any Nulls or not if yes convert it to N/A.
      - convert the data types to their valid forms like date for bal_dt, Int for id rather than simply adding string.
      - We also perform some level of filters and aggregations, like if field_x in ('a', 'b', 'c', 'd') then field_b = 'X'.
    - Consumption:
      - This layer is for business specific use cases.
      - Like each data platform will have multiple business teams. So if we perform entire business logic at the clean layer then it won't be reusable by other teams.
      - So We keep the separate consumption layer where we implement the business ready logics.
      - Based on the data available in consumption layer each team can run their specific queries.
      - If they are calculating a particular logic on daily bases then writing sql queries every day is a tedious task.
      - So instead of same sql queries they create KPI views.
      - And based on these views they can see their required filtered data on dashboard.
4. What is the difference between SCD and SCD 2 ? How can we create SCD explain with example ? What is SCD Type 2 ?
    - SCD means slowly change data.
    - Here SCD is applied for consumption which is gold layer.
    - SCD is applied for the entities which are getting changed very slowly over the time.
    - Here the entities like customer, restaurant, restaurant location, menu are the entities which will have the dimensions.
    - Here these dimensional entities will have very infrequent data changes, like a change in restaurant's menu is too infrequent.
    - So for these type of entities the SCD technique is applicable.
    - Here we have 6 types of SCD: SCD type 1, type 2, type 3, type 4, type 5, type 6.
    - In SCD type we just delete the previous unused record and update the entry with latest data.
    - In SCD type 2:
      - We use three main components:
        - Surrogate Key: For keep tracking of the activity of each record through the medallion architecture.
        - Effective Start and End Date: Historical status of each record
        - Is active flag: If the current record is active or not.
5. We have implemented SCD 2 in snowflake, but how can we do the same with parquet ?
   - Parquet is the most optimized file format we use while working with Big data pipelines.
   - But parquet files are immutable which means it doesn't support ACID properties.
   - But we have to used SCD type 2 in our dimension tables which means we have to find a way in which we can implement the same in pyspark.
   - So we can use the delta table build on top of delta lake. So we can use delta lake as a layer on parquet file to perform SCD Type 2 easily.
   - Delta table supports ACID properties.
6. What is Fact and the Dimension Table in data warehouse ?
    - Fact tables store quantitative data and measurements about business events. They contain the numeric values that can be analyzed to answer key business questions.
    - Dimension tables provide context and descriptive information about the data stored in the fact table.
    - Like the order item is fact table. But restaurant_hk, customer_hk are the foreign keys to the dimension tables.
    - So fact table will hold the foreign keys to all the dimension table.
7. What is delta table ?
    - Delta lake is an open-source storage layer that extends parquet file with ACID properties.
    - It is foundation for building lakehouse architecture.
    - It supports both batch processing and streaming.

8. How will you track the status of data load for each entity ?
    - We have designed all the pipelines in airflow.
    - We have a master dag which runs all the sub dags as a task.
    - These are the tasks:
      - Check Data availability.
      - Bronze Data Load.
      - Silver Data Load.
      - Gold Data Load (SCD for Dimension tables, Normal load for Transaction tables, Fact Table Update)
      - View Refresh Step.
      - Dashboard Refresh.
    - These airflow DAGs runs on kubernetes pods. So we can see the logs of each DAG run on the kubernetes pods.
    - Each DAG run will have their own unique correlation ID. We have integrated this correlation ID throughout the system.
    - By filtering this correlation ID we can check the logs in grafana dashboard. 
    - Here we have integrated the logging system where Open Telemetry collector sends the logs to grafana dashboard.

9. How we can handle the production failures ? Like how to check the data load progress, how we can identify that data got loaded successfully or not ?
    - We will run the pipelines on airflow in pyspark on kubernetes.
    - We can create a master airflow dag which will run each dah as a separate pipeline and inside each dag we will have a set of tasks.
    - At each task we can set the logs.
    - So we can see the job level errors in airflow dashboard.
    - Each dag will have driver and executor pods. We can track these errors in pods.
    - We can consider multiple scenarios:
        - Source System make the delay in providing the file for a given business date. In this case we can have a unique SLAs for each source system, If the SLA is not meet then we have to escalate that issue to the source system. And we have implemented incremental load in our system so all the other entities will execute in parallel upto clean layer. So no issues will occur at the time of consumption layer load.
        - We can set up the SLA alerting system. This system will keep track of SLA metric. Here we keep the metrics of each step. Delays from source system, Execution Time of each job. Data loading and processing Time. We can send this alert to the team in case of any issues.
        - Error in building pipelines. We might make some mistake at the time of building logic in dev environment. That's why we have DEV, UAT and PROD envs. UAT is PROD similar environment where we run the entire pipeline before moving to prod. So if there is some logical issue it will be fixed in UAT before moving to PROD.
        - We can implement logging system using OTEL collector and Grafana. Our entire system will have correlation ID. Each dag run will have its own correlation id. OTEL collector will supply the logs to the grafana dashboard where we can see the detailed logs of each dag run in detail based on correlation ID.
        - Source system gave jargon data. We have implemented data validation and quality checks in our silver layer. So in this case most likely the job will fail at silver layer. We can identify the issue and ask the source system to provide the file with correct values.
        - We can implement reconciliation process which keeps track of row count and calculate the sum of data from source to bronze to silver to gold. As we are doing initial load and delta load. So at the time of initial load we can ask business analyst for data validation. Through reconciliation process this can be done easily.
        - We can do a data recovery and data fail over. We can create a DR cluster which contains an exact replica of production where we can store the entire historical data. In case the production is down or data get corrupted we can have a huge historical backup of the data available on the spot. We just need to change the config file to run the dags on DR cluster instead of PROD.
        - Job fails due to OOM error. Sometimes we might get the data larger than our jobs can handle. In this case our job pods will fail with the OOM error. In this case we have two options. Either we change the processing engine configs with more driver and executor memory. Or we can scale our infra setup, We can add more node-groups, or we can level up the spot instances like m5a-4x-large.
        - We have multiple services running, the request payload will come from dataplane to control plane and the job will get triggered in dataplane. In this entire cycle any service might fail. In this case we must need a limited retry mechanism and accurately integrated logging system. So we can indentify where the failure exactly occurred.
        - Data Skewness: Performance issue might occur due to uneven data distribution in partitions. We need to resolve the data skewness issue at the time of developing the core logic.
        - Silent data corruption: Sometimes there are scenarios where data passes the quality checks, but it is not actually correct as per the business point of view. In this case we need to perform data anomaly checks.
        - Resource management: We can add the dynamic allocation properties in our jobs to make the optimum use of resources. In case customer runs the job with huge resources. But as we enable dynamic allocation in the backend it won't increase the usage. It will allocate the optimal set of resources.
