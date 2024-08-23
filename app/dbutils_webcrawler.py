import pyorient
    
client = pyorient.OrientDB("103.75.186.135", 2424)
client.set_session_token( True ) 
session_id = client.connect( "root", "root")

def OpenConnection():
    # Open the databasec
    db_name = "irs-search-db"
    if client.db_exists(db_name, pyorient.STORAGE_TYPE_MEMORY):
        client.db_open(db_name, "root", "root")
        print("Conenct to db successfully!")
    else:
        print(f"Database {db_name} does not exist.")
        exit()

def drop_db():
    try:
        if is_class_exists("Link"):                    
            client.command("DELETE EDGE Link;")
            client.command("DROP CLASS Link;")
            print('Drop Link successfully!')
        if is_class_exists("Website"):
            client.command("DELETE VERTEX Website;")                
            client.command("DROP CLASS Website;")
            print('Drop Website successfully!')
    except pyorient.exceptions.pyorientCommandException as e:
        # Bắt ngoại lệ và xử lý, nhưng không quăng lỗi tiếp
        print(f"Có lỗi xảy ra: {e}")

def is_class_exists(name):
    try:
        # Lấy danh sách các lớp hiện có trong cơ sở dữ liệu
        classes = client.command("SELECT FROM (SELECT expand(classes) FROM metadata:schema)")
        
        # Kiểm tra xem lớp 'Website' có trong danh sách hay không
        for cls in classes:
            if cls.name == name:
                print(f"Lớp {name} đã tồn tại.")
                return True
        return False
    except Exception as e:
        print(f"Lỗi khi kiểm tra lớp: {e}")
        return False

# Create the Website class
def create_website_class():    
    try:
        client.command("CREATE CLASS Website EXTENDS V")
        print('Create Website successfully!')
    except pyorient.exceptions.pyorientCommandException:
        # Class already exists
        pass

# Create the Link class
def create_link_class():
    try:
        client.command("CREATE CLASS Link EXTENDS E")
        print('Create Link successfully!')
    except pyorient.exceptions.pyorientCommandException:
        # Class already exists
        pass

# Insert a new website
def insert_website(id, url, title, desc):
    command = f"""
    CREATE VERTEX Website SET id = '{id}', url = '{url}', title = '{title}', description = '{desc}';
    """
    client.command(command)
    print(f"Insert website {id} successfully!")

# Insert a new link
def insert_link(from_id, to_id):
    command = f"""
    CREATE EDGE Link FROM (SELECT FROM Website WHERE id = '{from_id}') TO (SELECT FROM Website WHERE id = '{to_id}');    
    """
    client.command(command)
    print(f"Insert link from {from_id} to {to_id} successfully!")

# Read (Select) website
def read_website(condition=""):
    command = f"SELECT FROM Website {condition}"
    records = client.query(command)
    for record in records:
        print(record.oRecordData)

# OpenConnection()

# drop_db()
# create_website_class()
# create_link_class()

# insert_website('A', 'http://a.com', 'A', 'A')
# insert_website('B', 'http://b.com', 'B', 'B')
# insert_website('C', 'http://c.com', 'C', 'C')
# insert_website('D', 'http://d.com', 'D', 'D')

# insert_link('A', 'B')
# insert_link('A', 'C')
# insert_link('D', 'C')
# insert_link('D', 'C')
# insert_link('C', 'A')
# insert_link('B', 'C')

# read_website()