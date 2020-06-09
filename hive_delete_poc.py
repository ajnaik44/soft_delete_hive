from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import when

def rawSink(rowId):
    pass


def getLatestRecord(rowId):
    return  rowId


def raw(rowId):
    pass


def getMaskRecord(rowId):
    latestRecord =getLatestRecord(row)
    identifier_key = latestRecord['identifier_key']
    print(identifier_key)
    latestRecord = userDS.where(col(identifier_key)==row['identifier_value'])
    metadata = user_metadata_ds.filter(user_metadata_ds.usecase_name==row['usecase_name']).filter(user_metadata_ds.table_name==row['table_name'])
    metadataItr =   metadata.collect()
    for metaDataRow in  metadataItr:
        col_name = metaDataRow['column_name']
        col_value = metaDataRow['updated_value']
        s = list(set(latestRecord.columns) - {col_name})
        latestRecord = latestRecord.select(*s,when(col(col_name).isNotNull(),col_value).alias(col_name))
        
    return  latestRecord
    
def load_properties(filepath, sep='=', comment_char='#'):
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"') 
                props[key] = value 
    return props   

# this will be change with actual hive loading tables ...To DO
# configurable paramters will be done once all the code is done
if __name__=="__main__":
    user_tracker_config = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\usecase_tracker.csv")
    userDS_config = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\user.csv")
    user_metadata_ds_config = spark.read.option("header","true").csv(r"D:\clairvoyant\deletepoc\usecase_metadata.csv")
    
    user_tracker_config.registerTempTable("usecase_tracker_1")
    user_metadata_ds_config.registerTempTable("usecase_config")
    userDS_config.registerTempTable("users")
    
    
    configProp =load_properties(r"D:\clairvoyant\deletepoc\erase_config.properties")
    
    user_tracker =  spark.sql(configProp['usercase_tracker_query']);
    user_metadata_ds =  spark.sql(configProp['usercase_config_query']);
    user_tracker_record = user_tracker.collect()
    for row in user_tracker_record:
        getMaskRecord(row)
            
            
