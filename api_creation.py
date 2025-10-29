from flask import Flask, jsonify, request
import pandas as pd


# STEP 1: Load the data

df = pd.read_csv("cleaned_data.csv")


# STEP 2: Create Flask app

app = Flask(__name__)



# @app.route('/')
# def home():
#     return jsonify({"message": "Weather Data API!"})


@app.route('/data', methods=['GET'])
def get_all_data():
    return df.to_json(orient="records")

if __name__ == '__main__':
    app.run(debug=True) 




