# Apache-SPARK

Configure Docker Desktop
If you are using Windows or Mac, you need to change the settings to use as much resources as possible.

Go to Docker Desktop preferences.
Change memory to 12 GB.
Change CPUs to the maximum number.
Setup Environment
Here are the steps one need to follow to setup the lab.  

Clone the repository by running git clone https://github.com/itversity/spark-sql-and-pyspark-using-python3.git.
Pull the Image
Hadoop and Spark image is quite big. It is close to 1.5 GB.  

Make sure to pull it before running   
**docker-compose  ** 
command to setup the lab.
You can pull the image using   
**docker pull itversity/itvdelab.**
You can validate if the image is successfully pulled or not by running docker images command.
Start Environment
Here are the steps to start the environment.

  
Run **docker-compose up -d --build itvdelab**
It will set up single node Hadoop, Hive and Spark Environment along with metastore for hive.
You can run **docker-compose logs -f itvdelab **  
to review the progress. It will take some time to complete the setup process.
You can stop the environment using   
**docker-compose stop** command.
Access the Lab
Here are the steps to access the lab.

  
Make sure both Postgres and Jupyter Lab containers are up and running by using   
**docker-compose ps**
Get the token from the Jupyter Lab container using below command.  

**docker-compose exec itvdelab \
  sh -c "cat .local/share/jupyter/runtime/jpserver-*.json"**  
  
Use the token to login using http://localhost:8888/lab
Access Hadoop and Pyspark Material  

Once you login, you should be able to go through the third major module under itversity-material to access the content.
