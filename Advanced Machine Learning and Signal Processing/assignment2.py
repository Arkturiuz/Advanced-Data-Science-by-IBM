
# coding: utf-8

# This is the second assignment for the Coursera course "Advanced Machine Learning and Signal Processing"
# 
# Again, please insert to code to your ApacheCouchDB based Cloudant instance below using the "Insert Code" function of Watson Studio( you've done this in Assignment 1 before)
# 
# Done, just execute all cells one after the other and you are done - just note that in the last one you have to update your email address (the one you've used for coursera) and obtain a submission token, you get this from the programming assignment directly on coursera.
# 
# Please fill in the sections labelled with "###YOUR_CODE_GOES_HERE###"

# In[18]:


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

# In[19]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",credentials_2['custom_url'].split('@')[1])    .config("cloudant.username", credentials_2['username'])    .config("cloudant.password",credentials_2['password'])    .config("jsonstore.rdd.partitions", 1)    .getOrCreate()


# Now it’s time to have a look at the recorded sensor data. You should see data similar to the one exemplified below….
# 

# In[20]:


df=spark.read.load('shake_classification', "com.cloudant.spark")

df.createOrReplaceTempView("df")
spark.sql("SELECT * from df").show()


# Please create a VectorAssembler which consumed columns X, Y and Z and produces a column “features”
# 

# In[21]:


from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['X','Y','Z'], outputCol = 'features')


# Please insatiate a classifier from the SparkML package and assign it to the classifier variable. Make sure to either
# 1.	Rename the “CLASS” column to “label” or
# 2.	Specify the label-column correctly to be “CLASS”
# 

# In[36]:


from pyspark.ml.classification import LogisticRegression

classifier = LogisticRegression(labelCol = 'CLASS',maxIter=20, regParam=0.3, elasticNetParam=0.8)


# Let’s train and evaluate…
# 

# In[37]:


from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vectorAssembler, classifier])


# In[38]:


model = pipeline.fit(df)


# In[39]:


prediction = model.transform(df)


# In[40]:


prediction.show()


# In[41]:


from pyspark.ml.evaluation import MulticlassClassificationEvaluator
binEval = MulticlassClassificationEvaluator().setMetricName("accuracy") .setPredictionCol("prediction").setLabelCol("CLASS")
    
binEval.evaluate(prediction)


# If you are happy with the result (I’m happy with > 0.55) please submit your solution to the grader by executing the following cells, please don’t forget to obtain an assignment submission token (secret) from the Courera’s graders web page and paste it to the “secret” variable below, including your email address you’ve used for Coursera. (0.55 means that you are performing better than random guesses)
# 

# In[28]:


# !rm -Rf a2_m2.json
get_ipython().system(u'rm -Rf a2_m3.json')


# In[29]:


# prediction = prediction.repartition(1)
# prediction.write.json('a2_m2.json')
prediction= prediction.repartition(1)
prediction.write.json('a2_m3.json')


# In[30]:


# !rm -f rklib.py
# !wget https://raw.githubusercontent.com/romeokienzler/developerWorks/master/coursera/ai/rklib.py
get_ipython().system(u'rm -f rklib.py')
get_ipython().system(u'wget https://raw.githubusercontent.com/romeokienzler/developerWorks/master/coursera/ai/rklib.py')


# In[31]:


# !zip -r a2_m2.json.zip a2_m2.json
get_ipython().system(u'zip -r a2_m3.json.zip a2_m3.json')


# In[32]:


# !base64 a2_m2.json.zip > a2_m2.json.zip.base64
get_ipython().system(u'base64 a2_m3.json.zip > a2_m3.json.zip.base64')


# In[33]:


# from rklib import submit
# key = "J3sDL2J8EeiaXhILFWw2-g"
# part = "G4P6f"
# email = "air.reza@hotmail.com"
# secret = "CxRRKZe6sxGgnCgW"

# with open('a2_m2.json.zip.base64', 'r') as myfile:
#     data=myfile.read()
# submit(email, secret, key, part, [part], data)
from rklib import submit
key = "pPfm62VXEeiJOBL0dhxPkA"
part = "EOTMs"
email = "air.reza@hotmail.com"
secret = "F1cVYOf6C0PqgRjb"

with open('a2_m3.json.zip.base64', 'r') as myfile:
    data=myfile.read()
submit(email, secret, key, part, [part], data)

