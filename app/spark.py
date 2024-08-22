from databricks.connect import DatabricksSession
from graphframes import GraphFrame

def runSpark(data):  
    try:
        # Khởi tạo SparkSession
        spark = DatabricksSession.builder.remote(
            host       = "",
            token      = "",
            cluster_id = ""
        ).getOrCreate()
        
        # Nếu kết nối thành công
        print("Success: Connected to Databricks!")
        
        links = spark.createDataFrame(data)
        
        vertices = links.selectExpr("source as id", "sourceurl as url").distinct()
        edges = links.selectExpr("source as src", "target as dst")
        
        g = GraphFrame(vertices, edges)

        # Áp dụng thuật toán PageRank
        results = g.pageRank(resetProbability=0.15, maxIter=20)

        # Lấy top 10 trang web phổ biến nhất dựa trên PageRank
        top10_pages = results.vertices.orderBy("pagerank", ascending=False).limit(10)

        # Hiển thị kết quả
        top10_pages.show(truncate=False)

        return top10_pages
        
    except Exception as e:
        print(f"Error: {e}")
        return None