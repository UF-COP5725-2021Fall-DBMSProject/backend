from flask import Flask
from flask import jsonify
from flask import Blueprint
import query.c1_functions as c1Funcs

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
@c1_bp.route('/competitiveDrivers')
def c1_get_competitive_drivers():
    '''
        Comparators to Lewis, get the drivers who we want to compare with Lewis
        (ex. FunctionA over X% & FunctionB over Y% & Function C over Z%)
    '''
    result = {}
    return jsonify({"result":result})


@c1_bp.route('/funcA/<int:id>')
def c1a(id):
    # return _getData('./hy.json')
    q1, q2 = c1Funcs.c1_functiona(id)

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
    return jsonify({"result":result})

@c1_bp.route('/funcB/<int:id>')
def c1b(id):
    # return _getData('./hy.json')
    q = c1Funcs.c1_functionb(id)

    result = {}
    q = json.loads(q)
    result["data"] = q["data"]
    return jsonify({"result":result})


@c1_bp.route('/funcC/<int:id>')
def c1c(id):
    #TODO
    # q = c1Funcs.c1_functionc(id)

    result = {}
    # q = json.loads(q)
    # result["data"] = q["data"]
    return jsonify({"result":result})


if __name__ == '__main__':
    app.register_blueprint(example_bp, url_prefix='/example')
    app.register_blueprint(c1_bp, url_prefix='/c1')
    app.run(host='0.0.0.0', port=8000)