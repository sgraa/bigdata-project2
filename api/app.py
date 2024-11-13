from flask import Flask, request, jsonify
from pyspark.ml.classification import RandomForestClassificationModel  # Mengimpor RandomForestClassificationModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
MODEL_PATH = os.path.join("model", "models")

# Function to load model based on ID
def load_model(model_name):
    model_path = os.path.join(MODEL_PATH, model_name)
    print(f"Searching for model at: {model_path}")  # Log to verify model path

    if os.path.exists(model_path):
        try:
            # Load RandomForest model from directory using Spark MLlib
            model = RandomForestClassificationModel.load(model_path)
            return model
        except Exception as e:
            print(f"Failed to load model: {e}")
            return None
    else:
        print("Model not found.")
        return None

def prepare_features(data):
    # Encoding categorical features and including numeric values directly
    features = [
        0 if data.get("age", "<35") == "<35" else 1,  # Age: <35 = 0, >35 = 1
        1 if data.get("accessibility", "No") == "Yes" else 0,  # Accessibility: Yes = 1, No = 0
        0 if data.get("edlevel", "Undergraduate") == "Undergraduate" else 1,  # Education Level: Undergraduate = 0, Master = 1
        1 if data.get("employment", 1) == 1 else 0,  # Employment: Employed = 1, Unemployed = 0
        1 if data.get("gender", "Man") == "Man" else 0,  # Gender: Man = 1, Woman = 0
        1 if data.get("mentalhealth", "No") == "Yes" else 0,  # Mental Health: Yes = 1, No = 0
        1 if data.get("mainbranch", "Dev") == "Dev" else 0,  # Main Branch: Dev = 1, NonDev = 0
        data.get("yearscode", 0),  # Years of coding
        data.get("yearscodepro", 0),  # Years of professional coding
        data.get("previoussalary", 0),  # Previous Salary
        len(data.get("haveworkedwith", "").split(";")),  # Count of technologies worked with
        data.get("computerskills", 0)  # Computer Skills (directly as number)
    ]
    
    return Vectors.dense(features)

@app.route("/predict-employed/<model_id>", methods=["POST"])
def predict_employed(model_id):
    # Get JSON data from POST request
    data = request.json
    
    # Prepare input features for prediction
    input_vector = prepare_features(data)
    
    # Convert input vector to DataFrame with a single row
    input_df = spark.createDataFrame([(input_vector,)], ["features"])
    
    # Determine model name based on model_id
    model_name = f"rf_employment_model_{model_id}"  # Model name with batch ID
    model = load_model(model_name)

    if not model:
        return jsonify({"error": f"Model {model_id} not found"}), 404

    # Make prediction with the model
    prediction = model.transform(input_df)  # Transform input_df using the model
    
    # Extract predicted label (predicted category)
    predicted_label = prediction.select("prediction").head()[0]
    
    # Classify prediction result into categories: "Employed", "Unemployed", "Other"
    employment_status = "Other"
    if predicted_label == 0:
        employment_status = "Unemployed"
    elif predicted_label == 1:
        employment_status = "Employed"
    
    # Return prediction result as JSON
    return jsonify({
        "model_id": int(model_id),
        "employment_status": employment_status
    })

# Main program to run the API
if __name__ == "__main__":
    app.run(debug=True)
    spark.stop()
