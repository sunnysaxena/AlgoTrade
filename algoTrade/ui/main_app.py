import requests
# from my_fyers_model import MyFyersModel
from flask import request, render_template
from flask import Flask, jsonify

app = Flask(__name__)


# model = MyFyersModel()


@app.route('/')
def home():
    return 'Hello World!'


@app.route('/login', methods=['GET', 'POST'])
def login():
    json_data = request.json
    print(json_data)
    return jsonify(json_data['code'])


@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')


@app.route('/get_profile', methods=['GET', 'POST'])
def get_profile():
    pass


@app.route('/get_holdings', methods=['GET', 'POST'])
def get_holdings():
    pass


@app.route('/get_funds', methods=['GET', 'POST'])
def get_funds():
    pass


if __name__ == '__main__':
    app.run(debug=True)
