import os
import requests
import threading
import json
from flask import Flask, jsonify, render_template_string
from flask_restful import Api

def get_json(url):
    """
    This functions grabs the JSON file from the URL, parses it, and converts it to a Python dictionary.
    """

    try:
        # Get the JSON data
        response = requests.get(url)
        response.raise_for_status()

        # Parse the data
        data = response.json()

        # Convert into Python dictionary
        if isinstance(data, dict):
            pass
        elif isinstance(data, list):
            data = {"items": data}
        else:
            data = {"value": data}

        # Return the data
        print(f"Data received successfully from {url}: \n {data}")
        return data

    except requests.exceptions.HTTPError as requestError:
        code = requestError.response.status_code
        error = requestError

        # Handle connection errors
        if code == 404:
            error = f"Error 404: Unable to get JSON from {url} because page not found. \nError: {requestError}"
        elif code == 500:
            error = f"Error 500: Unable to get JSON from {url} because the other server is not working. \nError: {requestError}"
        else:
            error = f"Unknown Error {code}: Unable to get JSON from {url}. \nError: {requestError}"

        print(error)
        return error

    except ValueError as valueError:
        # If JSON data is not in valid format
        error = f"The JSON data is not valid.\n{valueError}"
        print(error)
        return error