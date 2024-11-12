# Spark Cluster Setup with Docker & PySpark Job Execution

This project provides instructions for setting up a Spark cluster with Docker containers and running a PySpark job to analyze tree data in `arbres.csv`.

## Prerequisites
- Docker
- A CSV file named `arbres.csv` containing tree data

## Steps

### 1. Set up Docker Network
Create a network for Spark containers to communicate with each other.
```bash
docker network create spark-network
```

### 2. Start Docker Containers for Spark Master and Workers
Run the following commands to create and start the master and worker containers:
```bash
docker run -itd --net=spark-network --name spark-master -p 8080:8080 liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd --net=spark-network --name spark-worker1 liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd --net=spark-network --name spark-worker2 liliasfaxi/spark-hadoop:hv-2.7.2
```

### 3. Configure Spark
- **Create the `slaves` file** in the Spark configuration directory on the master node:
    ```bash
    docker exec -it spark-master bash
    vi /usr/local/spark/conf/slaves
    ```
  
- **Add the worker nodes** to the `slaves` file:
    ```text
    spark-worker1
    spark-worker2
    ```

- **Start Spark services** on the master node:
    ```bash
    /usr/local/spark/sbin/start-all.sh
    ```

### 4. Modify Spark Configuration for Standalone Mode
Edit `spark-defaults.conf` and `core-site.xml` to configure Spark for local filesystem access.

### 5. Copy PySpark Job and Data to Spark Master
    ```bash
    docker cp C:\spark-data\spark_job.py spark-master:/usr/local/spark/data/spark_job.py
    docker cp C:\spark-data\arbres.csv spark-master:/usr/local/spark/data/arbres.csv
    ```

- **Set permissions** for `arbres.csv`:

    ```bash
    chmod 644 /usr/local/spark/data/arbres.csv
    ```

### 6. Run the PySpark Job
```bash
/usr/local/spark/sbin/spark-submit --master local[*] /usr/local/spark/data/spark_job.py
```

## Description of `spark_job.py`

This PySpark script performs the following:
1. Reads `arbres.csv` and displays a preview of the data.
2. Calculates the average tree height.
3. Identifies the tallest tree and its genre.
4. Counts the number of trees by genre.

## Access Spark UI
After starting the cluster, you can access the Spark UI at [http://localhost:8080](http://localhost:8080).


This README provides a concise overview, guiding users through setup and execution of the Spark job, and it includes a brief description of the PySpark script’s functionality.