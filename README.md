# airflow-dag-postgres-aws
Create an Airflow DAG to fetch articles metadata from Medium, transform, and load into postgresql and deploy on AWS EC2 and RDS, Airflow can be scheduled to run based on your preference. This project was made to load data into my other project, an NLP dashboard that analyzes text data to extract trending topics and relevant keywords, you can check it out at <a href="https://github.com/eyereece/nlp-text-mining-dashboard">NLP Dashboard repo</a>

Tutorial Companion: <a href="https://www.joankusuma.com/post/build-an-airflow-dag-on-aws-ec2">Build an Airflow DAG on AWS EC2</a><br>
<i>Tutorial companion for message queue will be coming soon</i>

# Architecture
<img src="./images/airflow2.png" width="900"/>
<br>
<br>

# Tech Stack
* Language: Python
* Data Pipeline:
    - Task scheduler: Airflow
    - Data Extraction: Scrapy 
    - Data Transformation: Pandas, NumPy
    - Data Loading: Pscopg2, RabbitMQ for queue worker.
* Deployment:
    - Airflow deployed on AWS EC2 with t3.medium
    - Postgres DB deployed on AWS RDS


<br>

# Getting Started
Please follow the steps in the tutorial linked above for best practices, particularly for setting up Airflow. Also, pay close attention to the paths. Start by setting up Airflow first (outlined in more details in the tutorial article) and start by running the hello_world_dag before trying others.
<br>

# References
<a href="https://github.com/dougsgrott/medium_scraper/tree/master">Medium Scraper Repo by Doug Sgrott </a>
