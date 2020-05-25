from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import when

user_tracker = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\usecase_tracker.csv").select("property","usecase_name","table_name","identifier_key", "identifier_value")
userDS = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\user.csv")

user_tracker_record = user_tracker.collect()



metadataDS = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\usecase_metadata.csv")

for row in user_tracker_record:
    metadata = metadataDS.filter(metadataDS.usecase_name==row['usecase_name'] ).filter(metadataDS.table_name==row['table_name'] )
    identifier_key_value = row['identifier_key']
    latestRecord =    userDS.where(col(identifier_key_value)==row['identifier_value'])
    print(metadata.select("column_name").collect()[0]['column_name'])
    latestRecord.show()
    latestRecordtoUpdate = latestRecord.select("*",when(latestRecord.is_deleted == "FALSE",metadata.select("updated_value").collect()[0][0]).alias(metadata.select("column_name").collect()[0][0]))
    latestRecordtoUpdate.show()
    #latesRecord.at[0,metadata.select("column_name").collect()[0][0]]= metadata.select("updated_value").collect()[0][0]
    #latesRecord.identifier_key_value = metadata.updated_value
    #userDS.where(identifier_key_value == row['identifier_value']).show
    
   
    
    
    
    
