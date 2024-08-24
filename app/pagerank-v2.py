import pyorient
import networkx as nx
from dbutils_pagerank import create_pagerank_class
from dbutils_pagerank import save_pagerank_to_orientdb

try:
    client = pyorient.OrientDB("103.75.186.135", 2424)
    session_id = client.connect( "root", "root")
    client.db_open("irs-search-db", "root", "root")
    print("connect ok")
except Exception as e:  
    print('LỖI')
    print(e)
# Lấy dữ liệu từ các vertedx (Website) và edge (Link)
vertices = client.command("SELECT id, url, title, description FROM Website")
edges = client.command("SELECT FROM Link")

# Tạo một đồ thị rỗng
G = nx.DiGraph()

# Thêm các đỉnh (vertex) vào đồ thị
for vertex in vertices:    
    vertex_data = vertex.oRecordData
    G.add_node(vertex_data['id'], id=vertex_data['id'], url=vertex_data['url'], title=vertex_data['title'], description=vertex_data['description'])

# Thêm các cạnh (edge) vào đồ thị
for edge in edges:
    from_id = client.command("SELECT id FROM Website WHERE @rid = {}".format(edge.oRecordData['out']))[0].oRecordData['id']
    to_id = client.command("SELECT id FROM Website WHERE @rid = {}".format(edge.oRecordData['in']))[0].oRecordData['id']
    G.add_edge(from_id, to_id)

# Tính toán PageRank
pagerank_scores = nx.pagerank(G, alpha=0.85)

# Tạo một dictionary mới để chứa thông tin chi tiết
pagerank = {}
for id, rank in pagerank_scores.items():
    pagerank[id] = {
        'id': id,
        'url': G.nodes[id]['url'],
        'title': G.nodes[id]['title'],
        'description': G.nodes[id]['description'],
        'pagerank': rank
    }

# Hiển thị kết quả
for data in pagerank.values():
    print(f"Website: {data['id']}, URL: {data['url']}, Title: {data['title']}, PageRank: {data['pagerank']}")

# Tạo lớp PageRank nếu chưa tồn tại
create_pagerank_class(client)

# Lưu kết quả PageRank vào OrientDB
save_pagerank_to_orientdb(client, pagerank)