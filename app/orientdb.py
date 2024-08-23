from pyorient import OrientDB
from config import ORIENTDB_DB_NAME,ORIENTDB_HOST,ORIENTDB_PASS,ORIENTDB_PORT,ORIENTDB_USER

def read_pagerank(client):
    """
    Đọc dữ liệu từ bảng PageRank trong OrientDB và trả về danh sách các từ điển chứa id, url, title, description, rank.
    """
    pagerank_list = []
    
    # Lấy dữ liệu từ bảng PageRank
    result = client.command("SELECT id, url, title, description, rank FROM PageRank")
    
    # Duyệt qua các kết quả và thêm vào danh sách
    for record in result:
        record_data = record.oRecordData
        pagerank_list.append({
            'id': record_data['id'],
            'url': record_data['url'],
            'title': record_data['title'],
            'description': record_data['description'],
            'rank': record_data['rank']
        })
    
    return pagerank_list

def connectOrientDB():
    client = OrientDB(ORIENTDB_HOST, ORIENTDB_PORT)
    session_id = client.connect(ORIENTDB_USER, ORIENTDB_PASS)
    client.db_open(ORIENTDB_DB_NAME, ORIENTDB_USER, ORIENTDB_PASS)
    
    print("connect ok")

    return client
