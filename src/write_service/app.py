import os
import json
from flask import Flask, render_template, render_template_string
from flask_restful import Api
from ingestion.fetch_data import get_json

# This is the Python app for the WRITE service
app = Flask(__name__)

@app.route('/')
def main():
    """For now, we just show a simple webpage."""
    print("The root directory of the write service has been accessed!")
    return render_template("index.html")

@app.route('/json')
def test_json():
    """
    Test function that pulls sample JSON data from the City of St. Louis website and then displays it to the user.
    """

    testURL = "https://www.stlouis-mo.gov/customcf/endpoints/arpa/expenditures.cfm?format=json"
    result = get_json(testURL)
    formattedResult = json.dumps(result, indent=2)
    html = f"""
        <html>
            <head>
                <title>STL Data API - Write Service - JSON Test</title>
            </head>
            <body>
                <h1>STL Data API - Write Service - JSON Test</h1>
                <h2>JSON from {testURL}</h2>
                <pre>{formattedResult}</pre>
            </body>
        </html>
    """
    return render_template_string(html)

@app.route('/health')
def health():
    """Endpoint for checking health of this app (if basic endpoint works or not)."""
    print("Health is okay.")
    return {'status': 'ok'}

if __name__ == '__main__':
    """Called when this app is started."""
    print("The write service Python app has started.")
    app.run(host='0.0.0.0', port=5050)