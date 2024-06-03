from flask import Flask
from dpma import dpma_bp
from merchant import merchant_bp
from mgr import mgr_bp

app = Flask(__name__)

# Register blueprints
app.register_blueprint(dpma_bp, url_prefix='/dpma')
app.register_blueprint(merchant_bp, url_prefix='/merchant')
app.register_blueprint(mgr_bp, url_prefix='/mgr')

if __name__ == '__main__':
    app.run(debug=True)
