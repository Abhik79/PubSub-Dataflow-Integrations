# Pub/Sub and Dataflow Integration for Real-time Data Processing

## Overview

This project demonstrates how to publish messages to Google Cloud Pub/Sub from a CSV file using Python, and how to consume those messages using an Apache Beam pipeline on Google Cloud Dataflow. The project is divided into two Jupyter notebooks:

1. **PubSub Publisher.ipynb**: Reads a CSV file and publishes each row as a message to a specified Pub/Sub topic.
2. **Dataflow Consumer.ipynb**: Consumes messages from a Pub/Sub subscription, processes them using Apache Beam, and writes the transformed data to a BigQuery table.

## Prerequisites

- Python 3.8 or above
- Google Cloud SDK installed and configured
- Service account key with appropriate permissions (Pub/Sub, Dataflow, BigQuery)
- `requirements.txt` installed

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Abhik79/PubSub-Dataflow-Integrations.git
cd yourrepository
```

### 2. Install Dependencies

Create a virtual environment and install the required dependencies using the `requirements.txt`:

The step is also provided inside bothe the Jupyter notebooks.
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Authentication

Place your Google Cloud service account JSON key in the `auth` folder and update the path in the notebooks:

```python
# Set the path to your service account key file in the notebooks
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../auth/<json-file-name>.json"
```

## Files

- **PubSub Publisher.ipynb**: Publishes messages from `employees.csv` to a Pub/Sub topic.
- **Dataflow Consumer.ipynb**: Reads messages from Pub/Sub, processes them, and writes them to a BigQuery table.

## Usage

### 1. Running Pub/Sub Publisher

1. Update the path to the `employees.csv` file in the `PubSub Publisher.ipynb` notebook.
2. Update the Pub/Sub topic path.
3. Run the notebook to publish messages to the Pub/Sub topic.

### 2. Running Dataflow Consumer

1. Authenticate with Google Cloud:

    ```bash
    gcloud auth application-default login
    ```

2. Update the project configurations in the `Dataflow Consumer.ipynb` notebook:
    - `PROJECT_ID`
    - `DATASET_ID`
    - `TABLE_NAME`
    - `BUCKET_NAME`
    - `REGION`
    - `SUBSCRIPTION`

3. Run the notebook to start the Dataflow job.

## BigQuery Schema

The Dataflow pipeline writes the data to a BigQuery table with the following schema:

- `insert_timestamp`: `TIMESTAMP`
- `first_name`: `STRING`
- `gender`: `STRING`
- `start_date`: `STRING`
- `last_login_time`: `STRING`
- `salary`: `FLOAT`
- `bonus_percent`: `FLOAT`
- `bonus_actual`: `FLOAT`
- `senior_management`: `BOOLEAN`
- `team`: `STRING`

## Notes

- Ensure that the Pub/Sub topic and subscription are properly set up in your Google Cloud project.
- The Dataflow pipeline uses a fixed window of 20 seconds for processing incoming messages.
- Modify the window size and SQL transformations as needed in the `Dataflow Consumer.ipynb` notebook.


## Contributing

Feel free to open issues or submit pull requests for improvements or bug fixes.

---

**Author**: [Your Name](https://github.com/yourusername)
```

Make sure to replace placeholders like `yourusername`, `yourrepository`, and `<json-file-name>` with your actual GitHub username, repository name, and the service account key filename, respectively. This `README.md` provides an overview of the project, setup instructions, and guidance on how to run the Jupyter notebooks.
