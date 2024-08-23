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
import subprocess
from dbutils_webcrawler import OpenConnection
from dbutils_webcrawler import create_website_class, create_link_class
from dbutils_webcrawler import insert_website, insert_link
from dbutils_webcrawler import read_website
from dbutils_webcrawler import drop_db

OpenConnection()
drop_db()
create_website_class()
create_link_class()

# Bước 1: Cài đặt chương trình web crawler thu thập nội dung các website theo cơ chế BFS
# Để bắt đầu chúng ta sẽ khai báo danh sách các URLs ban đầu chứa trong tập [seeds], 
# danh sách các URL đã duyệt qua [visited] nhằm để kiểm tra trùng lặp và chiều sâu tối đa [MAX_DEPTH]

# Khai báo danh sách tập seeds chứa các URLs ban đầu
seeds = [
    'https://vnexpress.net'
    #'https://dantri.com.vn'
    # 'https://vietnamnet.vn'
]

# Danh sách các URL đã truy cập
visited = []

# Chiều sâu duyệt tối đa của mỗi URL
MAX_DEPTH = 3

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
    if title is not None:
        title = title.text
        desc = desc.text
        content = content.text
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
       
def save_web_content(file_name, web_content):
    current_directory = os.path.dirname(os.path.abspath(__file__))   
    file_path = os.path.join(current_directory, 'web-crawler-output', file_name)        
    with open(file_path, 'w') as file:
        file.write(web_content)  
    
    # Upload the local file to HDFS
    # hdfs_file_path = f'hdfs://namenode:9000/user/hadoop/web-crawler-output/{file_name}'
    # subprocess.run(['hdfs', 'dfs', '-put', file_path, hdfs_file_path]) 
    
    # pip3 install hdfs
    # dfs.webhdfs.enabled true
    # Define HDFS file path
    # hdfs_file_path = f'/user/root/web-crawler/{file_name}'
    
    # Write content directly to HDFS
    # with hdfs_client.write(hdfs_file_path, encoding='utf-8') as writer:
    #     writer.write(web_content)                 

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
              if (base + path).endswith('#box_comment_vne'):
                print(' REJECTED: ' + base + path)
                continue              

              file_name = (base + path).split('/')[-1].replace('.html', '').replace('.htm', '') + '.txt'                            
              article_title, article_desc, article_content = get_article_content(html_content)
              if(file_name != '.txt' and len(article_content) > 0):
                print(' ACCEPTED: (base: ' + base + ', path: ' + path + ')')
                # Lưu tập tin .txt  
                save_web_content('(' + str(file_counter) + ')-' + file_name,article_content)                                  
                
                # Thêm vertex vào orientdb
                insert_website(page_id, page_url, article_title, article_desc)
                
                file_counter = file_counter + 1                                
                print(' [' + str(file_counter) + '] FILE SAVED: ' + file_name)                                

                # Xử lý lưu dữ liệu quan hệ giữa các website/webpage vào CSDL                
                # page (page_id) => hợp lệ
                # page (parent_id) => cần kiểm tra hợp lệ                
                print(' (parent id: ' + str(parent_id) + ', page id: ' + str(page_id) + ')')                                
                print(' (parent url: ' + parent_url + ', page url: ' + page_url + ')')
                if parent_id.isdigit():
                    web_edges.append((file_counter, parent_id, page_id, parent_url, page_url))
                    
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
                    
              # Lấy toàn bộ các thẻ <a> là các thẻ chứa hyper-links (href) của webpage đang đứng
              a_tags = html_content.find_all('a')

              # Duyệt qua từng hyper-links của webpage đang có
              for link in a_tags:

                  # Lấy ra đường dẫn liên kết trong attribute [href]
                  href = link.get('href')   
                  
                  parsed_url = urlparse(href)
                  fragment = parsed_url.fragment             
                  if fragment == 'box_comment_vne':
                      continue

                  # Kiểm tra xem đường dẫn này chúng ta đã duyệt qua hay chưa? thông qua đối chiếu trong danh sách [visited]
                  if href not in visited:

                      # Nếu chưa duyệt qua tiến hành bỏ hyper-link này vào [visited]
                      visited.append(href)
        
                      #print('Chiều sâu (depth) hiện tại: [{}/{}] - duyệt URL: [{}], '. format(depth, MAX_DEPTH, href))                      

                      # Xử lý lưu dữ liệu quan hệ giữa các website/webpage vào CSDL
                      parent_id = page_id
                      page_id = href.split('-')[-1].split('.')[0]
                      parent_url = page_url
                      page_url = href                      

                      # Kiểm tra xem đường dẫn này có phải là một đường dẫn hợp lệ - bắt đầu bằng http, ví dụ: https://vnexpress.net
                      if href.startswith('http'):
                          # Nếu hợp lệ - tiến hành bỏ hyper-link đang xét (href) vào [queue], và href sẽ là [base] mới, tăng chiều sâu [depth] lên 1
                          queue.append([page_id, page_url, parent_id, parent_url, href, '', depth + 1])
                      else:
                          # Nếu không hợp lệ - bỏ lại hyper-link cha [base] và (href) đang duyệt lại vào [queue] như cũ, tăng chiều sâu [depth] lên 1 
                          queue.append([page_id, page_url, parent_id, parent_url, base, href, depth + 1])
          except Exception as e:
            #pass
            print(f"Exception type: {type(e).__name__}")
            print(f"Exception message: {e}")
            traceback.print_exc()  # In chi tiết stack trace
     
# Bước 3: Tiến hành chạy thử nghiệm duyệt qua danh sách các URLs có trong tập seeds và 
# gọi hàm [fetch_by_bfs] để quét qua các URLs con trong mỗi websites theo cơ chế BFS

# Duyệt qua từng URL có trong tập seeds
for url in seeds:
  # Bỏ url này vào danh sách [visited]
  visited.append(url)
  # Khởi tạo một queue sau đó bỏ [url] vào - với chiều sâu mặc định là 0
  page_id, parent_id = 1, 0  # Khởi tạo page_id và parent_id cho lần đầu tiên
  page_url, parent_url = url, ''
  page_id = url.split('-')[-1].split('.')[0]
  queue = deque([[page_id, page_url, parent_id, parent_url, url, '', 0]])
  fetch_by_bfs(queue)
  
# Lưu dữ liệu quan hệ giữa các website/webpage vào CSV
save_web_edges(web_edges)

