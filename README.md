Step 1: Install Java (Spark is written in Scala, which runs on the Java Virtual Machine, or JVM).
  Go to https://adoptopenjdk.net and follow the download and installation instructions for downloading Java 8 or 11.
  
Step 2: Install Spark.
  Go on the Apache website (https://spark.apache.org/) and download the latest Spark release.
    1. Next, we need to download a winutils.exe to prevent some cryptic Hadoop errors. Go to the https://github.com/cdarlint/winutils repository and download the            winutils.exe file in the hadoop-X.Y.Z/bin directory where X.Y matches the Hadoop version that was used for the selected Spark version. Place the winutils.exe 
      in the bin directory of your Spark installation (C:\Users\[YOUR_USER_NAME\spark]).
    2. You will also need to set SPARK_HOME to the directory of your Spark installation (C:\Users\[YOUR-USER-NAME]\spark). Finally, add the %SPARK_HOME%\bin directory to your PATH environment variable.
    
Step 3: Install Python 3 and IPython.
  Go to https://www.anaconda.com/distribution and follow the installation instructions, making sure you’re getting the 64-bits graphical installer for Python 3.0 and above for your OS.
  
Step 4: Launch a PySpark shell using IPython.
        Create a dedicated virtual environment for PySpark, use the following command:

                $ conda create -n pyspark python=3.8 pandas ipython pyspark=3.2.0

                conda activate pyspark
                ipython

                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()

Step 5: (Optional) Install Jupyter and use it with PySpark.
          In your Anaconda PowerShell window, install Jupyter using the following command:

                $conda install -c conda-forge notebook

                cd [WORKING DIRECTORY]
                jupyter notebook
