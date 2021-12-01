from flask import Flask
from flask import jsonify
from flask import Blueprint
from flask import request
from werkzeug.wrappers import response
import query.c3_functions as c3Funcs

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
                "avg_pits": [
                    1.5, 2.0, 2.1
                ],
                "errors": [
                    1, 3, 2
                ]
            },
		    {
		        "driver_id": 1500,
		        "name": "Red Bull Racing",
                "total_point": [
                    60, 165, 300
                ],
                "Budgets": [
                    40, 50, 80
                ],
                "avg_pits": [
                    2.2, 2.3, 2.5
                ],
                "errors": [
                    5, 2, 4
                ]
		    },
      		{
		        "driver_id": 1000,
		        "name": "Toyota",
                "total_point": [
                    10, 30, 120
                ],
                "Budgets": [
                    10, 15, 25
                ],
                "avg_pits": [
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



### C3 API ###
@c3_bp.route('/top10defender')
def c3top10defender():
    
    q = c3Funcs.c3_function_get_top_10_defender_for_teammate()
    q = json.loads(q)

    data = {
        "name" : [],
        "driver_id" : [],
        "defend_point" : []
    }

    for d in q["data"]:
        data["name"].append(d["defender_forename"] + " " + d["defender_surname"])
        data["driver_id"].append(d["defender_driverid"])
        data["defend_point"].append(d["defend_point"])

    result = {}
    result["data"] = data
    # result["data"] = {
    #     999:{
    #         "forname": "Nick",
    #         "surname": "Young",
    #         "data":[
    #             {
    #                 "year":2019,
    #                 "race_name":"some race",
    #                 "race_id":9527,
    #                 "rival_id":487,
    #                 "rival_forname":"Ryan",
    #                 "rival_surname":"Kelly",
    #                 "success_defense_laps":[
    #                     {
    #                         "lap":5,
    #                         "position":6,
    #                         "rival_position":7
    #                     },
    #                     {
    #                         "lap":6,
    #                         "position":6,
    #                         "rival_position":7
    #                     },
    #                     {
    #                         "lap":7,
    #                         "position":6,
    #                         "rival_position":7
    #                     }
    #                 ]
    #             }
    #         ]
    #     }
    # }

    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response    

@c3_bp.route("/top10record/<int:id>")
def top10record(id):
    q = c3Funcs.c3_function_get_defender_best_10_records(id)
    q = json.loads(q)

    data = {
        "year" : [],
        "defender_name" : [],
        "defender_id": [],
        "opponent_id" : [],
        "opponent_name" : [],
        "teammate_id" : [],
        "teammate_name" : [],
        "race_id": [],
        "race_name" : [],
        "defend_point": []
    }

    for d in q["data"]:
        data["year"].append(d["year"])
        data["defender_name"].append(d["defender_forename"] + " " + d["defender_surname"])
        data["defender_id"].append(d["defender_driverid"])
        data["opponent_id"].append(d["victim_driverid"])
        data["opponent_name"].append(d["victim_forename"] + " " + d["victim_surname"])
        data["teammate_id"].append(d["teammate_driverid"])
        data["teammate_name"].append(d["teammate_forename"] + " " + d["teammate_surname"])
        data["race_id"].append(d["raceid"])
        data["race_name"].append(d["race_name"])
        data["defend_point"].append(d["defend_point"])

    result = {}
    result["data"] = data

    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@c3_bp.route("/recorddetail")
def c3recorddetaik():
    raceid = request.args.get("race_id")
    defender_id = request.args.get("defender_id")
    opponent_id = request.args.get("opponent_id")
    teammate_id = request.args.get("teammate_id")

    q = c3Funcs.c3_function_get_defender_record_detail(raceid, defender_id, opponent_id, teammate_id)
    q = json.loads(q)

    data = {
        "race_id":raceid,
        "defender_id":defender_id,
        "opponent_id":opponent_id,
        "teammate_id":teammate_id,
        "race_name":q["data"][0]["race_name"],
        "defender_name": q["data"][0]["defender_forename"] + " " + q["data"][0]["defender_surname"],
        "opponent_name": q["data"][0]["victim_forename"] + " " + q["data"][0]["victim_surname"],
        "teammate_name": q["data"][0]["teammate_forename"] + " " + q["data"][0]["teammate_surname"],

        "lap":[],
        "defender_position":[],
        "opponent_position":[],
        "teammate_position":[]
    }

    for d in q["data"]:
        data["lap"].append(d["index"]+1)
        data["defender_position"].append(d["defender_position"])
        data["opponent_position"].append(d["victim_position"])
        data["teammate_position"].append(d["teammate_position"])

    result = {}
    result["data"] = data

    response = jsonify({"result":result})
    if app.debug:
        # [Important] Let web are able to hit the domain 'localhost'
        response.headers.add('Access-Control-Allow-Origin', '*')
    return response

if __name__ == '__main__':
    app.register_blueprint(example_bp, url_prefix='/example')
    app.register_blueprint(c1_bp, url_prefix='/c1')
    app.register_blueprint(c2_bp, url_prefix='/c2')
    app.register_blueprint(c3_bp, url_prefix='/c3')
    app.run(host='0.0.0.0', port=8000, debug=True)