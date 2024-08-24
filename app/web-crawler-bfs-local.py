# Web crawler thường sử dụng BFS (Breadth-First Search) thay vì DFS (Depth-First Search).
# - BFS giúp thu thập dữ liệu từ các trang web gần gốc hơn trước, 
#   điều này hữu ích để đảm bảo rằng các trang quan trọng, thường liên kết trực tiếp với nhiều trang khác, được thu thập sớm.
# - DFS có thể đi sâu vào một nhánh duy nhất, bỏ lỡ các trang quan trọng gần gốc, 
#   và có nguy cơ bị mắc kẹt trong các vòng lặp hoặc các nhánh sâu không quan trọng.
# => BFS thường là lựa chọn tốt hơn cho web crawler.

import os
import csv
import requests
from bs4 import BeautifulSoup
from collections import deque
from urllib.parse import urlparse
import traceback
import re
#from hdfs import InsecureClient
from dbutils_webcrawler import OpenConnection
from dbutils_webcrawler import create_website_class, create_link_class
from dbutils_webcrawler import insert_website, insert_link
from dbutils_webcrawler import read_website
from dbutils_webcrawler import drop_db

# Bước 1: Cài đặt chương trình web crawler thu thập nội dung các website theo cơ chế BFS
# Để bắt đầu chúng ta sẽ khai báo danh sách các URLs ban đầu chứa trong tập [seeds], 
# danh sách các URL đã duyệt qua [visited] nhằm để kiểm tra trùng lặp và chiều sâu tối đa [MAX_DEPTH]

# Khai báo danh sách tập seeds chứa các URLs ban đầu
seeds = [
    #'https://tinhte.vn',
    #'https://tinhte.vn/thread/xiaomi-da-ban-hon-27-000-chiec-xe-dien-trong-quy-2-chiu-lo-hon-9-000-usd-moi-chiec.3820280/'
    'https://vnexpress.net',
    #'https://vnexpress.net/robert-f-kennedy-jr-dinh-chi-tranh-cu-tuyen-bo-ung-ho-ong-trump-4785098.html'    # nhiều link con kết hợp link cha có dạng vnexpress.net/abc.htm/tin-tuc
    #'https://vnexpress.net/them-nhieu-truong-dai-hoc-xet-tuyen-bo-sung-4784769.html' # mối quan hệ phức tạp nhất (vòng tròn)
    #'https://dantri.com.vn'
    # 'https://vietnamnet.vn'
    # 'https://vnexpress.net/gia-xang-moi-nhat-hom-nay-22-8-4784451.html'                 # có 1 quan hệ
    #'https://vnexpress.net/hang-tram-nguoi-trai-nghiem-tai-ai4vn-2024-4784766.html'     # không có quan hệ
    #'https://dantri.com.vn/xa-hoi/nhan-cau-cuu-tu-tai-xe-csgt-voi-dung-xe-dac-chung-cho-nguoi-di-vien-20240823105202045.htm'
]

# Danh sách các URL đã truy cập
visited = []

# Chiều sâu duyệt tối đa của mỗi URL
MAX_DEPTH = 3

OpenConnection()
drop_db()
create_website_class()
create_link_class()

# vnexpress.net site:    
    # h1 class title-detail
    # p class desctiption
    # article class= fck_detail
# dantri.com.vn
    # h1 class title-page detail
    # h2 class singular-sapo
    # div class singular-content
# html_content: a beautifulsoup object
def get_article_content(html_content):        
    # vnexpress    
    title = html_content.find('h1', {'class': 'title-detail'})
    desc = html_content.find('p', {'class': 'description'})
    content = html_content.find('article', {'class': 'fck_detail'})
    if title is None:
        # dantri
        title = html_content.find('h1', {'class': 'title-page detail'})
        desc = html_content.find('h2', {'class': 'singular-sapo'})
        content = html_content.find('div', {'class': 'singular-content'})        
    # if title is None:
    #     # tinhte        
    #     title = html_content.select_one('div[class*="thread-title"]')
    #     #title = html_content.find('div', class_=lambda x: x and 'thread-title' in x)                
    #     content = html_content.find('article') 
    #     desc = content
    #     if title is not None:
    #         print('title: ', title.text)        
    if title is not None:
        title = title.text.replace('"', '').replace("'", "")
        desc = desc.text.replace('"', '').replace("'", "")
        content = content.text.replace('"', '').replace("'", "")
        #print(f'title: ' + title)                     
        return title, desc, (title + '\n' +  desc + '\n' + content)
    return '', '', ''

def save_web_edges(web_edges):    
    file_name = 'web-edges.csv'            
    # Lấy thư mục hiện tại của tập tin .py
    current_directory = os.path.dirname(os.path.abspath(__file__))   
    file_path = os.path.join(current_directory, 'web-crawler-output', file_name)                       
    # Lưu danh sách edges vào tệp tin edges.csv với header
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        # Viết header
        writer.writerow(['No', 'Source', 'Target', "SourceUrl", "TargetUrl", "SourceTitle", "SourceDesc"])
        # Viết các dòng dữ liệu
        writer.writerows(web_edges)

# Create an HDFS client
# hdfs_client = InsecureClient('hdfs://namenode:9000', user='root')

def save_web_content(file_name, web_content):
    current_directory = os.path.dirname(os.path.abspath(__file__))   
    file_path = os.path.join(current_directory, 'web-crawler-output', file_name)        
    with open(file_path, 'w') as file:
        file.write(web_content)
    
    # Upload the local file to HDFS
    # hdfs_file_path = f'hdfs://namenode:9000/user/root/web-crawler/{file_name}'
    # subprocess.run(['hdfs', 'dfs', '-put', file_path, hdfs_file_path]) 
    
    # pip3 install hdfs
    # dfs.webhdfs.enabled true
    # Define HDFS file path
    # hdfs_file_path = f'/user/root/web-crawler/{file_name}'
    
    # Write content directly to HDFS
    # with hdfs_client.write(hdfs_file_path, encoding='utf-8') as writer:
    #     writer.write(web_content)                 

def get_id_from_url(url):
    # Chuyển đổi đầu vào thành chuỗi, nếu cần
    if not isinstance(url, str):
        url = str(url)
            
    # Biểu thức chính quy để tìm tất cả các chuỗi số có ít nhất 4 chữ số
    pattern = r'\d{4,}'
    
    # Tìm tất cả các số trong chuỗi s
    numbers = re.findall(pattern, url)
    
    if numbers:
        return numbers[-1]  # Lấy số cuối cùng từ danh sách
    else:
        return ''  # Nếu không tìm thấy số nào, trả về '

# Bước 2: Xây dựng hàm [fetch_by_bfs] có thể quét qua các URLs trong tập seeds theo cơ chế BFS

# Viết hàm duyệt theo cơ chế BFS để tiến hành quét qua các URLs trong tập seeds theo
file_counter = 1
web_edges = []
def fetch_by_bfs(queue):
    global file_counter
    global web_edges
    while queue:
        # Lấy thông tin về: URL gốc (base), URL đang duyệt và chiều sâu (depth) ra khỏi queue
        page_id, page_url, parent_id, parent_url, base, path, depth = queue.popleft()      
        page_url = (base + path)
        # Kiểm tra xem chiều sâu hiện tại đã vượt quá [MAX_DEPTH] hay chưa
        if depth < MAX_DEPTH:
            try:
                # Tải nội dung ở định dạng <html> của webpage hiện tại đang đứng
                # Ở bước này chúng ta có thể lưu nội dung của website vào CSDL để phục vụ cho việc chỉ mục sau này
                try:
                    html_content = BeautifulSoup(requests.get(base + path).text, 'html.parser')
                except:
                    continue                            
                
                print('CURRENT: (base: ' + base + ', path: ' + path + ')')

                # Bỏ qua link trùng nội dung webpage hiện đang đứng                
                if path.endswith('.htm') or path.endswith('.html') or path == '':
                    rejected = False                
                if path.endswith('#box_comment_vne'):
                    rejected = True
                if rejected == True:       
                    print(' REJECTED: ' + base + path)
                    continue 
                
                page_valid = False
                file_name = (base + path).split('/')[-1].replace('.html', '').replace('.htm', '') + '.txt'                            
                article_title, article_desc, article_content = get_article_content(html_content)              
                if(page_id.isdigit() and file_name != '.txt' and len(article_title) > 0):
                    print(' ACCEPTED: (base: ' + base + ', path: ' + path + ')')
                    page_valid = True
                    
                    # Lưu tập tin .txt  
                    save_web_content('(' + str(file_counter) + ')-' + file_name, article_content) 
                    
                    # Thêm vertex vào orientdb
                    insert_website(page_id, page_url, article_title, article_desc)
                                                                                                        
                    print(' [' + str(file_counter) + '] FILE SAVED: ' + file_name)                                
                    file_counter = file_counter + 1                                

                    # Xử lý lưu dữ liệu quan hệ giữa các website/webpage vào CSDL                
                    # page (page_id) => hợp lệ
                    # page (parent_id) => cần kiểm tra hợp lệ                
                    print(' (parent id: ' + str(parent_id) + ', page id: ' + str(page_id) + ')')                                
                    print(' (parent url: ' + parent_url + ', page url: ' + page_url + ')')
                    if page_id.isdigit() and parent_id.isdigit():
                        web_edges.append((file_counter, parent_id, page_id, parent_url, page_url, article_title, article_desc ))
                        
                        # Thêm edge vào orientdb
                        try:
                            insert_link(f'{parent_id}', f'{page_id}')
                        except Exception as e:
                            print(e)
                        
                        print(' EDGE VALID: [' + str(parent_id) + ']->[' + str(page_id) + ']')                    
                    else:
                        print(' EDGE INVALID: [' + str(parent_id) + ']->[' + str(page_id) + ']')
                    
                else:
                    # Xóa bỏ tính hợp lệ của page hiện tại (with page_id)
                    page_id = ''
                    
                #print('parent: ' + page_url)
                
                # Lấy toàn bộ các thẻ <a> là các thẻ chứa hyper-links (href) của webpage đang đứng
                a_tags = html_content.find_all('a')

                # Duyệt qua từng hyper-links của webpage đang có
                for link in a_tags:                  

                    # Lấy ra đường dẫn liên kết trong attribute [href]
                    href = link.get('href')
                    
                    if href is None:
                        continue
                    if href == '/' or href == 'javascript:;':
                        continue
                    
                    parsed_url = urlparse(href)
                    fragment = parsed_url.fragment             
                    if fragment == 'box_comment_vne':
                        continue

                    #print('  child: ' + href)

                    # Kiểm tra xem đường dẫn này chúng ta đã duyệt qua hay chưa? thông qua đối chiếu trong danh sách [visited]
                    if href not in visited:

                        # Nếu chưa duyệt qua tiến hành bỏ hyper-link này vào [visited]
                        visited.append(href)
        
                        #print('Chiều sâu (depth) hiện tại: [{}/{}] - duyệt URL: [{}], '. format(depth, MAX_DEPTH, href))                      

                        # Xử lý lưu dữ liệu quan hệ giữa các website/webpage vào CSDL                                            
                        child_parent_id = page_id                        
                        #child_page_id = href.split('-')[-1].split('.')[0]
                        child_page_id = get_id_from_url(href)
                        if child_page_id is None:
                            continue
                        child_parent_url = page_url
                        child_page_url = href
                        #print(' (parent id: ' + str(child_parent_id) + ', page id: ' + str(child_page_id) + ')')                                                            
                        
                        # Kiểm tra xem đường dẫn này có phải là một đường dẫn hợp lệ - bắt đầu bằng http, ví dụ: https://vnexpress.net
                        if href.startswith('http'):                            
                            # Nếu hợp lệ - tiến hành bỏ hyper-link đang xét (href) vào [queue], và href sẽ là [base] mới, tăng chiều sâu [depth] lên 1
                            queue.append([child_page_id, child_page_url, child_parent_id, child_parent_url, href, '', depth + 1])
                        else:
                            # Nếu url trang cha đã hợp lệ, và url trang con không bắt đầu http thì ko cần xét tiếp
                            if page_valid == True:
                                continue
                            # Nếu không hợp lệ - bỏ lại hyper-link cha [base] và (href) đang duyệt lại vào [queue] như cũ, tăng chiều sâu [depth] lên 1 
                            queue.append([child_page_id, child_page_url, child_parent_id, child_parent_url, base, href, depth + 1])                  
            except Exception as e:
                #pass
                print(f"Exception type: {type(e).__name__}")
                print(f"Exception message: {e}")
                traceback.print_exc()  # In chi tiết stack trace
            # if file_counter == 2:
            #     return
     
# Bước 3: Tiến hành chạy thử nghiệm duyệt qua danh sách các URLs có trong tập seeds và 
# gọi hàm [fetch_by_bfs] để quét qua các URLs con trong mỗi websites theo cơ chế BFS

# Duyệt qua từng URL có trong tập seeds
for url in seeds:
  # Bỏ url này vào danh sách [visited]
  visited.append(url)
  # Khởi tạo một queue sau đó bỏ [url] vào - với chiều sâu mặc định là 0
  page_id, parent_id = '', ''  # Khởi tạo page_id và parent_id cho lần đầu tiên
  page_url, parent_url = url, ''
  page_id = url.split('-')[-1].split('.')[0]
  queue = deque([[page_id, page_url, parent_id, parent_url, url, '', 0]])
  fetch_by_bfs(queue)
  
# Lưu dữ liệu quan hệ giữa các website/webpage vào CSV
save_web_edges(web_edges)

#read_website()
