from flask import Flask
from flask import jsonify
from flask import Blueprint
from flask import request
from werkzeug.wrappers import response
import query.c1_functions as c1Funcs
import query.c2_functions as c2Funcs

# testing
import json
def _getData(file):
    with open(file) as f:
        data = json.load(f)
    result = {}
    columns = []

    for d in data["schema"]["fields"]:
        if d["name"] != "index":
            columns.append(d["name"])
    for d in data["data"]:
        del d["index"]

    result["columns"] = columns
    result["data"] = data["data"]
    return {"result":result}
# /testing
app = Flask(__name__)
@app.route('/api/', methods=['GET', 'POST'])
def api():
    return jsonify({'status': 0,
    'result': {}})

example_bp = Blueprint('example',__name__)

@example_bp.route('/year/<int:year>')
def example_year(year):
    return jsonify({'example':year})


c1_bp = Blueprint('C1',__name__)
c2_bp = Blueprint('C2',__name__)
c3_bp = Blueprint('C3',__name__)
c4_bp = Blueprint('C4',__name__)

### C1 API ###
@c1_bp.route('/competitive-drivers')
def c1_get_competitive_drivers():
    '''
        Comparators to Lewis, get the drivers who we want to compare with Lewis
        (ex. FunctionA over X% & FunctionB over Y% & Function C over Z%)
    '''
    # mock data
    result = {
        "lewis_id": 1,
        "drivers":[
            {
                "driver_id": 2000,
		        "name": "Anmol"
            },
		    {
		        "driver_id": 1500,
		        "name": "YiMing"
		    },
            {
		        "driver_id": 1000,
		        "name": "Ryan Huang"
		    },
            {
		        "driver_id": 999,
		        "name": "Jim Chou"
		    }
        ]
    }

    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@c1_bp.route('/funcA/<int:id>')
def c1_a(id):
    # return _getData('./hy.json')
    q1, q2 = c1Funcs.c1_function_a(id)

    result = {}
    columns = []
    q1 = json.loads(q1)

    for d in q1["schema"]["fields"]:
        if d["name"] != "index":
            columns.append(d["name"])
    for d in q1["data"]:
        del d["index"]

    # print(type(q1))
    # return jsonify(q1)
    result["columns"] = columns
    result["data"] = q1["data"]
    
    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@c1_bp.route('/funcB/<int:id>')
def c1_b(id):
    # return _getData('./hy.json')
    q = c1Funcs.c1_function_b(id)

    result = {}
    q = json.loads(q)
    result["data"] = q["data"]
    
    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response



@c1_bp.route('/funcC/<int:id>')
def c1_c(id):
    #TODO
    # q = c1Funcs.c1_function_c(id)

    result = {}
    # q = json.loads(q)
    # result["data"] = q["data"]
    # [Important] Let web are able to hit the domain 'localhost'
    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response


### C2 API ###
@c2_bp.route('/investable-constructors')
def c2_get_investable_constructors():
    '''
        List the investable constructors
        (Constructors who match the conditions)
    '''
    # mock data

    start_year = request.args.get('start_year')
    end_year = request.args.get('end_year')
    if not start_year:
        start_year = 2015
    if not end_year:
        end_year = 2017
    

    q1, q2, q3, q4 = c2Funcs.c2_function(start_year,end_year)
    q1 = json.loads(q1)
    q2 = json.loads(q2)
    q3 = json.loads(q3)
    q4 = json.loads(q4)
    q2_constructor_set = set([d["constructor_id"] for d in q2["data"]])
    q3_constructor_set = set([d["constructor_id"] for d in q3["data"]])
    q4_constructor_set = set([d["constructor_id"] for d in q4["data"]])

    union_constructor_set = q2_constructor_set & q3_constructor_set & q4_constructor_set
    rankPoints = {}
    for id in union_constructor_set:
        rankPoints[id] = 0
    
    totalPoints = {}
    for d in q1["data"]:
        # print(d["total_points"])
        if d["constructor_id"] not in totalPoints:
            totalPoints[d["constructor_id"]] = 0
        else:
            totalPoints[d["constructor_id"]] += d["total_points"]

    i = 0
    for id in totalPoints:
        if id in union_constructor_set:
            rankPoints[id] += i
        i += 1

    i = len(totalPoints)
    for d in q3["data"]:
        if d["constructor_id"] in union_constructor_set:
            rankPoints[d["constructor_id"]] += i
        i -= 1

    final_rank = {key: rank for rank, key in enumerate(sorted(rankPoints, key=rankPoints.get, reverse=True), 1)}

    # print(final_rank)

    needed_attributes = {"budgets","avg_pits_time","constructor_id","name","total_points"}
    constructors = []
    for d in final_rank:
        constructor = {}
        constructor["rank"] = final_rank[d]
        for k in needed_attributes:
            constructor[k] = [d_in_q1[k] for d_in_q1 in q1["data"] if d_in_q1['constructor_id'] == d]
        constructor["constructor_id"] = constructor["constructor_id"][0]
        constructor["name"] = constructor["name"][0]
        constructors.append(constructor)



    # query range start_year ~ end_year, determine by client
    # return the value with "ASC" order by year 
    result = {}
    result["data"] = {"constructors":constructors}



    # result["data"] = {
    #     "constructors":[
    #         {
    #             "constructor_id": 2000,
		  #       "name": "Benz",
    #             "total_points": [
    #                 100, 150, 220
    #             ],
    #             "Budgets": [
    #                 20, 45, 70
    #             ],
    #             "avg_pits": [
    #                 1.5, 2.0, 2.1
    #             ],
    #             "errors": [
    #                 1, 3, 2
    #             ]
    #         },
		  #   {
		  #       "driver_id": 1500,
		  #       "name": "Red Bull Racing",
    #             "total_points": [
    #                 60, 165, 300
    #             ],
    #             "Budgets": [
    #                 40, 50, 80
    #             ],
    #             "avg_pits": [
    #                 2.2, 2.3, 2.5
    #             ],
    #             "errors": [
    #                 5, 2, 4
    #             ]
		  #   },
    #   		{
		  #       "driver_id": 1000,
		  #       "name": "Toyota",
    #             "total_points": [
    #                 10, 30, 120
    #             ],
    #             "Budgets": [
    #                 10, 15, 25
    #             ],
    #             "avg_pits": [
    #                 2.2, 2.3, 2.5
    #             ],
    #             "errors": [
    #                 10, 15, 8
    #             ]
		  #   }
    #     ]
    # }

    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == '__main__':
    app.register_blueprint(example_bp, url_prefix='/example')
    app.register_blueprint(c1_bp, url_prefix='/c1')
    app.register_blueprint(c2_bp, url_prefix='/c2')
    app.run(host='0.0.0.0', port=8000, debug=True)