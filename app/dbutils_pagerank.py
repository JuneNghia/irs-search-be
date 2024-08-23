import pyorient

def class_exists(client, class_name):
    """Kiểm tra xem class đã tồn tại hay chưa."""
    try:
        client.command(f"SELECT FROM {class_name} LIMIT 1")
        return True
    except pyorient.exceptions.PyOrientCommandException:
        return False

def create_pagerank_class(client):
    """Tạo class PageRank. Nếu đã tồn tại, xóa class trước khi tạo lại."""
    if class_exists(client, "PageRank"):
        client.command("DROP CLASS PageRank UNSAFE")
        print("Class PageRank đã tồn tại. Đã xóa class cũ.")
    try:
        client.command("CREATE CLASS PageRank EXTENDS V")
        client.command("CREATE PROPERTY PageRank.id STRING")
        client.command("CREATE PROPERTY PageRank.url STRING")
        client.command("CREATE PROPERTY PageRank.title STRING")
        client.command("CREATE PROPERTY PageRank.description STRING")
        client.command("CREATE PROPERTY PageRank.rank DOUBLE")
        print("Class PageRank đã được tạo mới.")
    except pyorient.exceptions.PyOrientCommandException as e:
        # Class already exists, ignore
        print(f"Exception: {e}")
    
def save_pagerank_to_orientdb(client, pagerank):
    for data in pagerank.values():
        query = "INSERT INTO PageRank (id, url, title, description, rank) VALUES ('{}', '{}', '{}', '{}', {})".format(data['id'], data['url'], data['title'], data['description'], data['pagerank'])
        client.command(query)
