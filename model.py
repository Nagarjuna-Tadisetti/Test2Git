import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType,ArrayType
from pyspark.sql import SparkSession
import pickle
import pandas as pd
from sklearn.externals import joblib
import numpy as np

#Change the file path

spark = SparkSession.builder.appName('pyspark').getOrCreate()
knn_model=pickle.load(open('/home/sumit123/finalized_model.pkl','rb'))
model_broadcast = spark.sparkContext.broadcast(knn_model)


def model_predict(*cols):
    np_features = np.array([cols])
    return model_broadcast.value.predict(np_features)[0].item()


model_udf = f.udf(model_predict, IntegerType())



#Change the path here
input_data=spark.read.csv("file:///home/sumit123/Desktop/FreeLancer/sci-kit-spark/src/data.csv",header=False,inferSchema=True)

df_pred_b = input_data.withColumn("prediction",model_udf(f.col('_c0'),f.col('_c1'),f.col('_c2'),f.col('_c3'))
)

df_pred_b.show()
