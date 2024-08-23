import os
import re
import numpy as np
from scipy.spatial import distance
from sklearn.feature_extraction.text import TfidfVectorizer
from .orientdb import read_pagerank, connectOrientDB

def do_search(keyword):
    client = connectOrientDB()
    
    current_dir = os.path.dirname(os.path.abspath(__file__))

    folder_path = os.path.join(current_dir, 'web-crawler-output')  # Đường dẫn tới thư mục chứa các tập tin txt
    docs = []  # Danh sách để lưu nội dung các tập tin
    filenames = []  # Danh sách để lưu tên các tập tin

    # Duyệt qua tất cả các tập tin trong thư mục
    c = 1
    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(folder_path, filename)
            # print(filename)
            with open(file_path, 'r') as file:
                # Đọc nội dung và loại bỏ ký tự xuống dòng
                content = file.read().replace('\n', ' ')
                # Loại bỏ nhiều khoảng trắng thành 1 khoảng trắng
                content = re.sub(r'\s+', ' ', content)
                docs.append(content)
                filenames.append(filename)
                c += 1
        if c > 100:
            break

    #print(docs)

    # Truy vấn (q)
    #query = 'information retrieval'
    # query = 'Phương Thị Thu'
    query = keyword

    # Để cho tiện lợi trong việc xử lý chúng ta sẽ gán câu truy vấn là 1 tài liệu cuối cùng
    docs.append(query)

    doc_len = len(docs)

    # Khởi tạo đối tượng TfidfVectorizer
    vectorizer = TfidfVectorizer()

    # Tiến hành chuyển đổi các tài liệu/văn bản và truy vấn về dạng các vector TF-IDF
    # [tfidf_matrix] là một ma trận ở dạng thưa (sparse) - chỉ lưu các vị trí có giá trị khác 0  - chứa trọng số TF-IDF của các tài liệu/văn bản
    tfidf_matrix = vectorizer.fit_transform(docs)

    # Chúng ta tiến hành chuyển ma trận tfidf_matrix về dạng đầy đủ
    tfidf_matrix = tfidf_matrix.todense()

    # Lấy danh sách tập từ vựng
    vocab = vectorizer.get_feature_names_out()
    vocab_size = len(vocab)
    # print('Kích thước tập từ vựng: [{}]'.format(vocab_size))
    # print('Tập từ vựng (V):')
    # print(vocab)

    # Chuyển đổi  ma trận (numpy) về dạng list
    tfidf_matrix_list = tfidf_matrix.tolist()

    # Danh sách để lưu trữ thông tin tài liệu và các giá trị tương đồng
    similarity_list = []

    # TFIDF encode của truy vấn (q) là tài liệu cuối cùng
    # Kết quả tính TF-IDF của thư viện Scikit-Learn sẽ hơi khác với cách tính truyền thống
    # vì IDF của Scikit-Learn sẽ là: idf(t) = loge [ (1+n) / ( 1 + df ) ] + 1
    # sau đó toàn bộ ma trận TF-IDF sẽ được bình thường hoá lại với (norm - L2)
    # tuy nhiên kết quả cuối cùng cũng sẽ không thay đổi
    query_tfidf_encoded_vector = tfidf_matrix_list[doc_len-1]
    #print(query_tfidf_encoded_vector)

    # Xóa query đã được mã hóa thành dạng tfidf vector ra khỏi tfidf_matrix_list
    del tfidf_matrix_list[doc_len-1]

    # Duyệt qua danh sách các tài liệu/văn bản (đã mã hóa ở dạng  vectors)
    for doc_idx, doc_tfidf_encoded_vector in enumerate(tfidf_matrix_list):

        # Tính tích vô hướng giữa hai vectors tài liệu và truy vấn
        dot_product_sim = np.dot(query_tfidf_encoded_vector, doc_tfidf_encoded_vector)

        # Tính khoảng cách Euclid giữa hai vectors tài liệu và truy vấn
        ed = distance.euclidean(query_tfidf_encoded_vector, doc_tfidf_encoded_vector)
        
        # Tính tương đồ cosine giữa hai vectors tài liệu và truy vấn
        cs = 1 - distance.cosine(query_tfidf_encoded_vector, doc_tfidf_encoded_vector)
        
        # Lưu trữ thông tin vào danh sách
        similarity_list.append((doc_idx, filenames[doc_idx], dot_product_sim, ed, cs))

        # print('Tài liệu: [{}], tương đồng (dot product): [{:.6f}]'.format(doc_idx, dot_product_sim))
        # print('Tài liệu: [{}], tương đồng (khoảng cách Euclid): [{:.6f}]'.format(doc_idx, ed))
        # print('Tài liệu: [{}], tương đồng (Tương đồng cosine): [{:.6f}]'.format(doc_idx, cs))
        # print('---')
    
    # Sắp xếp danh sách theo dot_product_sim giảm dần
    sorted_similarity_list = sorted(similarity_list, key=lambda x: x[2], reverse=True)

    # In kết quả sau khi sắp xếp
    # print('Top 10 tài liệu tương đồng: ')
    for doc_info in sorted_similarity_list:
        doc_idx, filename, dot_product_sim, ed, cs = doc_info
        # print('File: [{}], dot product: [{:.6f}], \n  filename: {}'.format(doc_idx, dot_product_sim, filename))
    
    #return sorted_similarity_list[:10]
    
    # Đọc dữ liệu từ bảng PageRank
    pagerank_data = read_pagerank(client)
    
    # Chuyển pagerank_data thành dictionary với id làm khóa
    pagerank_dict = {record['id']: record for record in pagerank_data}
    
    # Kết hợp similarity_list và pagerank_dict
    combined_list = []
    for sim in similarity_list:
        id = sim[0]  # Lấy id từ phần tử đầu tiên của tuple                
        filename = sim[1]        
        dot_product = sim[2]
        euclid = sim[3]
        cosine = sim[4]
        id = filename.split('-')[-1].replace('.txt', '')        
        if id in pagerank_dict:            
            combined_record = {
                'id': id,
                'url': pagerank_dict[id]['url'],
                'title': pagerank_dict[id]['title'],
                'description': pagerank_dict[id]['description'],
                'rank': pagerank_dict[id]['rank'],
                'dot-product': dot_product,
                'euclid': euclid,
                'cosine': cosine
            }
            combined_list.append(combined_record)
    
    # Sắp xếp combined_list trước tiên theo 'dot-product' và sau đó theo 'rank'
    combined_list_sorted = sorted(combined_list, key=lambda x: (x['dot-product'], x['rank']), reverse=True)

    # Trả về danh sách đã kết hợp đã được sắp xếp
    # for record in combined_list_sorted:
    print(combined_list_sorted)

    return combined_list_sorted[:10]