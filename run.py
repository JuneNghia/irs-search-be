from flask import Flask, request, jsonify
from app.search import do_search

app = Flask(__name__)

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('query', '').lower()
    # Filter items based on the query
    result = do_search(query)
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)

# /search?query=<