from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import when

# this will be change with actual hive loading tables ...To DO
# configurable paramters will be done once all the code is done
user_tracker = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\usecase_tracker.csv").select("property","usecase_name","table_name","identifier_key", "identifier_value")
userDS = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\user.csv")

user_tracker_record = user_tracker.collect()
userDS = spark.read.option("header","true").csv(r"E:\ClairvoyantWork\DeleteUseCase\user.csv")
user_metadata_ds = spark.read.option("header","true").csv(r"E:\ClairvoyantWork\DeleteUseCase\usecase_metadata.csv")
for row in user_tracker_record:
    metadata = user_metadata_ds.filter(user_metadata_ds.usecase_name==row['usecase_name']).filter(user_metadata_ds.table_name==row['table_name'])
    identifier_key = row['identifier_key']
    print(identifier_key)
    latestRecord = userDS.where(col(identifier_key)==row['identifier_value'])
    updated_col_detail = metadata.select("column_name","updated_value").collect();
    col_name = updated_col_detail[0]['column_name']
    col_value = updated_col_detail[0]['updated_value']
    s = list(set(latestRecord.columns) - {col_name})
    latestRecord.select(*s,when(col(col_name).isNotNull(),col_value).alias(col_name)).show()
