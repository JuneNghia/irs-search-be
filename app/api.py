from flask import Flask, request, jsonify
from flask_restful import Api, Resource
from flasgger import Swagger

app = Flask(__name__)
api = Api(app)
swagger = Swagger(app)


class Search(Resource):
    def get(self):
        """
        This method responds to the GET request for this endpoint and returns the data in OrientDB after calculate TF-IDF and Page rank.
        ---
        tags:
        - Search
        parameters:
            - name: query
              in: query
              type: string
              required: true
              description: Search with TF-IDF and Page rank
        responses:
            200:
                description: A successful GET request
                content:
                    application/json:
                      schema:
                        type: object
                        properties:
                            text:
                                type: string
                                description: Search
        """
        query = request.args.get('query')
        
        if not query: 
            return jsonify({"message": 'Yêu cầu tham số query'})
        else:
            return jsonify({"query": query.upper()})
    
api.add_resource(Search, "/search")

if __name__ == "__main__":
    app.run(debug=True)