from flask import Flask
from flask import jsonify
from flask import Blueprint
app = Flask(__name__)
@app.route('/api/', methods=['GET', 'POST'])
def api():
    return jsonify({'status': 0,
    'result': {}})

example_bp = Blueprint('example',__name__)

@example_bp.route('/year/<int:year>')
def example_year(year):
    return jsonify({'example':year})


if __name__ == '__main__':
    app.register_blueprint(example_bp, url_prefix='/example')
    app.run(host='0.0.0.0', port=8000)