
from databricks.connect import DatabricksSession

def runSpark():  
    try:
        # Khởi tạo SparkSession
        spark = DatabricksSession.builder.remote(
            host       = "",
            token      = "",
            cluster_id = ""
        ).getOrCreate()
        
        # Nếu kết nối thành công
        print("Success: Connected to Databricks!")
        
        df = spark.createDataFrame(
            [
                ("sue", 32),
                ("li", 3),
                ("bob", 75),
                ("heo", 13),
            ],
            ["first_name", "age"]
        )
        
        df.show()
        
        return spark
    except Exception as e:
        print(f"Error: {e}")
        return None