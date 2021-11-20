from flask import Flask
from flask import jsonify
from flask import Blueprint
from flask import request
from werkzeug.wrappers import response
import query.c1_functions as c1Funcs
import query.c4_functions as c4Funcs
import copy

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
    # for d in q1["data"]:
    #     del d["index"]

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
    
    # query range start_year ~ end_year, determine by client
    # return the value with "ASC" order by year 
    result = {}
    result["data"] = {
        "constructors":[
            {
                "constructor_id": 2000,
		        "name": "Benz",
                "total_point": [
                    100, 150, 220
                ],
                "Budgets": [
                    20, 45, 70
                ],
                "avg_pits_time": [
                    1.5, 2.0, 2.1
                ],
                "errors": [
                    1, 3, 2
                ]
            },
		    {
		        "constructor_id": 1500,
		        "name": "Red Bull Racing",
                "total_point": [
                    60, 165, 300
                ],
                "Budgets": [
                    40, 50, 80
                ],
                "avg_pits_time": [
                    2.2, 2.3, 2.5
                ],
                "errors": [
                    5, 2, 4
                ]
		    },
      		{
		        "constructor_id": 1000,
		        "name": "Toyota",
                "total_point": [
                    10, 30, 120
                ],
                "Budgets": [
                    10, 15, 25
                ],
                "avg_pits_time": [
                    2.2, 2.3, 2.5
                ],
                "errors": [
                    10, 15, 8
                ]
		    }
        ]
    }

    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response


### C4 API ###
@c4_bp.route('/funcA/<int:id>')
def c4a(id):
    q = c4Funcs.c4_function_a(id)
    q = json.loads(q)

    data = {
        "name" : "",
        "driver_id" : 0,
        "year" : [],
        "points" : [],
        "crash" : []   
    }
    data["name"] = q["data"][0]["forename"] + " " + q["data"][0]["surname"]
    data["driver_id"] = q["data"][0]["driverid"]

    other_attr = {"year","points","crash"}
    for d in q["data"]:
        for attr in other_attr:
            data[attr].append(d[attr])

    result = {}
    result["data"] = data

    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@c4_bp.route('/crashing-driver-lists')
def c4_get_aggressive_driver():
    useless, risky, aggressive = c4Funcs.c4_function_get_useless_risky_aggresive_drivers_list()

    useless = json.loads(useless)
    risky = json.loads(risky)
    aggressive = json.loads(aggressive)


    list_template = {
        "name" : [],
        "driver_id" : [],
        "points" : [],
        "crashes" : [],
        "ratio" : []
    }

    result = {}
    result["data"] = {}
    result["data"]["useless"] = copy.deepcopy(list_template)
    result["data"]["risky"] = copy.deepcopy(list_template)
    result["data"]["aggressive"] = copy.deepcopy(list_template)

    for d in useless["data"]:
        result["data"]["useless"]["name"].append(d["forename"] + " " + d["surname"])
        result["data"]["useless"]["driver_id"].append(d["driverid"])
        result["data"]["useless"]["points"].append(d["total_point"])
        result["data"]["useless"]["crashes"].append(d["total_crash"])
        result["data"]["useless"]["ratio"].append(d["total_point"]/d["total_crash"])

    for d in risky["data"]:
        result["data"]["risky"]["name"].append(d["forename"] + " " + d["surname"])
        result["data"]["risky"]["driver_id"].append(d["driverid"])
        result["data"]["risky"]["points"].append(d["total_point"])
        result["data"]["risky"]["crashes"].append(d["total_crash"])
        result["data"]["risky"]["ratio"].append(d["crash_devide_by_point"])

    for d in aggressive["data"]:
        result["data"]["aggressive"]["name"].append(d["forename"] + " " + d["surname"])
        result["data"]["aggressive"]["driver_id"].append(d["driverid"])
        result["data"]["aggressive"]["points"].append(d["total_point"])
        result["data"]["aggressive"]["crashes"].append(d["total_crash"])
        result["data"]["aggressive"]["ratio"].append(d["point_devide_by_crash"])

    # result["data"]["useless"] = useless["data"]
    # result["data"]["risky"] = risky["data"]
    # result["data"]["aggressive"] = aggressive["data"]

    # result["data"]["useless"] = {
    #     "name" : ["Jim","Ryan"],
    #     "driver_id" : [888,777],
    #     "points" : [0,0],
    #     "crashes" : [20,20],
    #     "ratio" : [0,0]
    # }
    # result["data"]["risky"] = {
    #     "name" : ["YM","Anmol"],
    #     "driver_id" : [888,777],
    #     "points" : [100,100],
    #     "crashes" : [1,2],
    #     "ratio" : [0.01,0.02]
    # }
    # result["data"]["aggressive"] = {
    #     "name" : ["Jim","Ryan"],
    #     "driver_id" : [888,777],
    #     "points" : [20,20],
    #     "crashes" : [2,4],
    #     "ratio" : [10,5]
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
    app.register_blueprint(c4_bp, url_prefix='/c4')

    app.run(host='0.0.0.0', port=8000, debug=True)