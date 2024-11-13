from flask import Flask, request, jsonify
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
import os

# Set environment variables to use 'python' as the interpreter
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

# Initialize Flask App
app = Flask(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PredictionAPI") \
    .master("local[*]") \
    .getOrCreate()

# Path for the model
MODEL_PATH = "D:/tugas/smst5/big data/project-big-data/model/models/"

# In-memory storage for prediction history
prediction_history = []

# Function to load model based on ID
def load_model(model_name):
    model_path = os.path.join(MODEL_PATH, model_name)
    if os.path.exists(model_path):
        try:
            model = RandomForestClassificationModel.load(model_path)
            return model
        except Exception as e:
            print(f"Failed to load model: {e}")
            return None
    else:
        print("Model not found.")
        return None


def prepare_features(data):
    features = [
        0 if data.get("age", "<35") == "<35" else 1,
        1 if data.get("accessibility", "No") == "Yes" else 0,
        0 if data.get("edlevel", "Undergraduate") == "Undergraduate" else 1,
        1 if data.get("employment", 1) == 1 else 0,
        1 if data.get("gender", "Man") == "Man" else 0,
        1 if data.get("mentalhealth", "No") == "Yes" else 0,
        1 if data.get("mainbranch", "Dev") == "Dev" else 0,
        data.get("yearscode", 0),
        data.get("yearscodepro", 0),
        data.get("previoussalary", 0),
        len(data.get("haveworkedwith", "").split(";")),
        data.get("computerskills", 0)
    ]
    return Vectors.dense(features)

@app.route("/predict-employed/<model_id>", methods=["POST"])
def predict_employed(model_id):
    data = request.json
    input_vector = prepare_features(data)
    
    model_name = f"rf_employment_model_{model_id}"
    model = load_model(model_name)

    if not model:
        return jsonify({"error": f"Model {model_id} not found"}), 404

    # Make a direct prediction on the input vector
    prediction_result = model.predict(input_vector)

    # Convert prediction to human-readable employment status
    employment_status = "Other"
    if prediction_result == 0:
        employment_status = "Unemployed"
    elif prediction_result == 1:
        employment_status = "Employed"
    
    # Log the prediction to the history
    prediction_entry = {
        "model_id": model_id,
        "input_data": data,
        "employment_status": employment_status
    }
    prediction_history.append(prediction_entry)

    return jsonify({
        "model_id": int(model_id),
        "employment_status": employment_status
    })

@app.route("/predict-multi-models", methods=["POST"])
def predict_multi_models():
    data = request.json
    model_ids = data.get("model_ids", [])
    input_vector = prepare_features(data)

    results = []
    for model_id in model_ids:
        model_name = f"rf_employment_model_{model_id}"
        model = load_model(model_name)
        
        if not model:
            results.append({"model_id": model_id, "error": "Model not found"})
            continue

        prediction_result = model.predict(input_vector)

        employment_status = "Other"
        if prediction_result == 0:
            employment_status = "Unemployed"
        elif prediction_result == 1:
            employment_status = "Employed"

        # Append prediction to history
        prediction_entry = {
            "model_id": model_id,
            "input_data": data,
            "employment_status": employment_status
        }
        prediction_history.append(prediction_entry)

        results.append({
            "model_id": model_id,
            "employment_status": employment_status
        })

    return jsonify(results)

@app.route("/aggregate-predict", methods=["POST"])
def aggregate_predict():
    data = request.json
    model_ids = data.get("model_ids", [])

    # Initialize counters for employment predictions
    employed_count = 0
    unemployed_count = 0

    # Filter predictions in history based on specified model_ids
    for entry in prediction_history:
        if int(entry["model_id"]) in model_ids:
            if entry["employment_status"] == "Employed":
                employed_count += 1
            elif entry["employment_status"] == "Unemployed":
                unemployed_count += 1

    # Return the aggregated results
    return jsonify({
        "model_ids": model_ids,
        "employed_count": employed_count,
        "unemployed_count": unemployed_count
    })

@app.route("/history", methods=["GET"])
def get_prediction_history():
    return jsonify(prediction_history)


if __name__ == "__main__":
    app.run(debug=True)
    spark.stop()
