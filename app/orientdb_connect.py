from pyorient import OrientDB
from config import ORIENTDB_DB_NAME,ORIENTDB_HOST,ORIENTDB_PASS,ORIENTDB_PORT,ORIENTDB_USER

def get_data_from_orientdb():
    client = OrientDB(ORIENTDB_HOST, ORIENTDB_PORT)
    session_id = client.connect(ORIENTDB_USER, ORIENTDB_PASS)
    client.db_open(ORIENTDB_DB_NAME, ORIENTDB_USER, ORIENTDB_PASS)

    result = client.command("SELECT * FROM web_edges")
    
    data = [record.oRecordData for record in result]

    client.db_close()

    return data
