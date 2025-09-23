from flask import Flask, render_template
from flask_restful import Api

# This is the Python app for the WRITE service
app = Flask(__name__)

@app.route('/')
def main():
    """For now, we just show a simple webpage."""
    print("The root directory of the write service has been accessed!")
    return render_template("index.html")

@app.route('/health')
def health():
    """Endpoint for checking health of this app (if basic endpoint works or not)."""
    print("Health is okay.")
    return {'status': 'ok'}

if __name__ == '__main__':
    """Called when this app is started."""
    print("The write service Python app has started.")
    app.run(host='0.0.0.0', port=5050)