
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook. Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# Sampling is one of the most important things when it comes to visualization because often the data set get so huge that you simply
# 
# - can't copy all data to a local Spark driver (Data Science Experience is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[13]:


def getSample(df,spark):
    # ANS: df.sample(False, 0.1)
    return df.sample(False, 0.1)


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and retur a python list containing all temperature values from the data set

# In[14]:


def getListForHistogramAndBoxPlot(df,spark):
    return spark.sql("select temperature from washing where temperature is not null").rdd.map(lambda row : row.temperature).collect()


# Finally we want to create a run chart. Please return two lists (encapusalted in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refere to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[15]:


#should return a tuple containing the two lists for timestamp and temperature
#please make sure you take only 10% of the data by sampling
#please also ensure that you sample in a way that the timestamp samples and temperature samples correspond (=> call sample on an object still containing both dimensions)
def getListsForRunChart(df,spark):
    temp = spark.sql("select temperature, ts from washing where temperature is not null order by ts asc")
    temp_rdd = temp.rdd.sample(False, 0.1).map(lambda row : (row.ts, row.temperature))
    ts_array = temp_rdd.map(lambda (ts, temperature) : ts).collect()
    temp_array = temp_rdd.map(lambda (ts, temperature) : temperature).collect()
    dataset_list = ts_array, temp_array
    return dataset_list


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code
# 
# ### TODO Please provide your Cloudant credentials here

# In[16]:


### TODO Please provide your Cloudant credentials here by creating a connection to Cloudant and insert the code
### Please have a look at the latest video "Connect to Cloudant/CouchDB from ApacheSpark in Watson Studio" on https://www.youtube.com/c/RomeoKienzler
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same
user = "14603069-106b-4ab7-9b3b-96dfc3c36b0f-bluemix"
pw = "9c13726ef87aa211fe818f5d002f1b23c721fe15601d4c8829a2d69c953fba4f"
hostname = "14603069-106b-4ab7-9b3b-96dfc3c36b0f-bluemix.cloudantnosqldb.appdomain.cloud"


# In[17]:


#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "com.cloudant.spark")

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# In[18]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host", hostname)    .config("cloudant.username", user)    .config("cloudant.password", pw)    .config("jsonstore.rdd.partitions", 1)    .getOrCreate()


# In[19]:


df=readDataFrameFromCloudant(database)


# In[20]:


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt


# In[21]:


plt.hist(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[22]:


plt.boxplot(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[23]:


lists = getListsForRunChart(df,spark)


# In[24]:


plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! Please download the notebook as python file, name it assignment4.1.py and sumbit it to the grader.
