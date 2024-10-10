from flask import Flask, Blueprint
from BluePrints.crashes_bp import crash_bp

app = Flask(__name__)

app.register_blueprint(crash_bp, url_prefix='/api')


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run(debug=True)
