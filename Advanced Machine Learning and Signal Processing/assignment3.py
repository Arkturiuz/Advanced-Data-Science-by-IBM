
# coding: utf-8

# This is the third assignment for the Coursera course "Advanced Machine Learning and Signal Processing"
# 
# Again, please insert to code to your ApacheCouchDB based Cloudant instance below using the "Insert Code" function of Watson Studio (you've done this in Assignment 1 and 2 before)
# 
# Done, just execute all cells one after the other and you are done - just note that in the last one you must update your email address (the one you've used for coursera) and obtain a submission token, you get this from the programming assignment directly on coursera.
# 
# Please fill in the sections labelled with "###YOUR_CODE_GOES_HERE###"
# 

# In[80]:


#your cloudant credentials go here
###YOUR_CODE_GOES_HERE###"
# @hidden_cell
credentials_2 = {
  'password':"""3aea02f3f55bdc873ddf2c94d9fe82d4f3a7bd5b1ba9321d2990c48517fe2d23""",
  'custom_url':'https://a4af65b5-e83e-4e5e-85ea-cebc73a44b31-bluemix:3aea02f3f55bdc873ddf2c94d9fe82d4f3a7bd5b1ba9321d2990c48517fe2d23@a4af65b5-e83e-4e5e-85ea-cebc73a44b31-bluemix.cloudantnosqldb.appdomain.cloud',
  'username':'a4af65b5-e83e-4e5e-85ea-cebc73a44b31-bluemix',
  'url':'https://undefined'
}


# Let's create a SparkSession object and put the Cloudant credentials into it

# In[81]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",credentials_2['custom_url'].split('@')[1])    .config("cloudant.username", credentials_2['username'])    .config("cloudant.password",credentials_2['password'])    .config("jsonstore.rdd.partitions", 1)    .getOrCreate()


# Now it’s time to have a look at the recorded sensor data. You should see data similar to the one exemplified below….
# 

# In[82]:


df=spark.read.load('shake_classification', "com.cloudant.spark")

df.createOrReplaceTempView("df")
spark.sql("SELECT * from df").show()


# Let’s check if we have balanced classes – this means that we have roughly the same number of examples for each class we want to predict. This is important for classification but also helpful for clustering

# In[83]:


spark.sql("SELECT count(class), class from df group by class").show()


# Let's create a VectorAssembler which consumes columns X, Y and Z and produces a column “features”
# 

# In[84]:


from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols=["X","Y","Z"], 
                                  outputCol="features")


# Please insatiate a clustering algorithm from the SparkML package and assign it to the clust variable. Here we don’t need to take care of the “CLASS” column since we are in unsupervised learning mode – so let’s pretend to not even have the “CLASS” column for now – but it will become very handy later in assessing the clustering performance. PLEASE NOTE – IN REAL-WORLD SCENARIOS THERE IS NO CLASS COLUMN – THEREFORE YOU CAN’T ASSESS CLASSIFICATION PERFORMANCE USING THIS COLUMN 
# 
# 

# In[85]:


from pyspark.ml.clustering import GaussianMixture
clust = GaussianMixture().setK(2).setSeed(1)


# Let’s train...
# 

# In[86]:


from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vectorAssembler, clust])
model = pipeline.fit(df)


# ...and evaluate...

# In[87]:


prediction = model.transform(df)
prediction.show()


# In[88]:


prediction.createOrReplaceTempView('prediction')
spark.sql('''
select max(correct)/max(total) as accuracy from (

    select sum(correct) as correct, count(correct) as total from (
        select case when class != prediction then 1 else 0 end as correct from prediction 
    ) 
    
    union
    
    select sum(correct) as correct, count(correct) as total from (
        select case when class = prediction then 1 else 0 end as correct from prediction 
    ) 
)
''').rdd.map(lambda row: row.accuracy).collect()[0]


# If you reached at least 55% of accuracy you are fine to submit your predictions to the grader. Otherwise please experiment with parameters setting to your clustering algorithm, use a different algorithm or just re-record your data and try to obtain. In case you are stuck, please use the Coursera Discussion Forum. Please note again – in a real-world scenario there is no way in doing this – since there is no class label in your data. Please have a look at this further reading on clustering performance evaluation https://en.wikipedia.org/wiki/Cluster_analysis#Evaluation_and_assessment
# 

# In[89]:


get_ipython().system(u'rm -Rf a2_m3.json')


# In[90]:


get_ipython().system(u'rm -f rklib.py')
get_ipython().system(u'wget https://raw.githubusercontent.com/romeokienzler/developerWorks/master/coursera/ai/rklib.py')


# In[91]:


prediction= prediction.repartition(1)
prediction.write.json('a2_m3.json')


# In[92]:


get_ipython().system(u'zip -r a2_m3.json.zip a2_m3.json')


# In[93]:


get_ipython().system(u'base64 a2_m3.json.zip > a2_m3.json.zip.base64')


# In[94]:


from rklib import submit
key = "pPfm62VXEeiJOBL0dhxPkA"
part = "EOTMs"
email = "air.reza@hotmail.com"
secret = "dBHh8j1uxoDDJcvn"

with open('a2_m3.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, secret, key, part, [part], data)

