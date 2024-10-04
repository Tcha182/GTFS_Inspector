# GTFS Inspector

GTFS Inspector is a Streamlit app designed for the teams of RATP Dev to inspect the content of GTFS RT for their subsidiaries. The app allows users to fetch and visualize real-time vehicle and trip data from GTFS RT feeds.

## Features

- Fetch GTFS RT data for vehicle positions and trip updates.
- Visualize vehicle positions on an interactive map.
- Filter data by Vehicle ID, Trip ID, or Route ID.
- Download the fetched data in JSON and Excel (XLSX) formats.
- Manage GTFS RT URLs (Add, Modify, Delete).

## Installation

1. **Clone the repository:**

    ```bash
    git clone https://github.com/Tcha182/GTFS_Inspector.git
    cd GTFS_Inspector
    ```

2. **Create a virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install the required packages:**

    ```bash
    pip install -r requirements.txt
    ```

4. **Set up your Streamlit secrets:**

    Create a `.streamlit` directory and add a `secrets.toml` file with your Google Cloud Platform service account credentials:

    ```toml
    [gcp_service_account]
    type = "service_account"
    project_id = "your_project_id"
    private_key_id = "your_private_key_id"
    private_key = "your_private_key"
    client_email = "your_client_email"
    client_id = "your_client_id"
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "your_client_x509_cert_url"
    ```

## Usage

1. **Run the Streamlit app:**

    ```bash
    streamlit run GTFS_Inspector.py
    ```

2. **Interact with the app:**

    - Use the sidebar to manage GTFS RT URLs (Add, Modify, Delete).
    - Select a GTFS RT feed and fetch the data.
    - Filter the data by **Vehicle ID**, **Trip ID**, or **Route ID**.
    - View vehicle positions on the interactive map.
    - Download the fetched data in **JSON** or **Excel (XLSX)** formats.


## Functions

- `upload_or_update_network_file(network_name, content)`: Uploads or updates a network configuration file in Google Cloud Storage.
- `download_network_file(network_name)`: Downloads a network configuration file from Google Cloud Storage.
- `open_gtfs_realtime_from_url(url)`: Fetches GTFS real-time data from a given URL.
- `get_filtered_data(vehicle_data, trip_data, filter_option, selected_value)`: Filters vehicle and trip data based on the selected filter type (Vehicle ID, Trip ID, or Route ID).
- `create_map(filtered_vehicle_data)`: Creates an interactive map showing the filtered vehicle positions.
- `flatten_dict(d, parent_key='', sep='_')`: Flattens a nested dictionary for easier data processing.
- `protobuf_to_dataframe(feed)`: Converts GTFS protobuf messages into a Pandas DataFrame.


## Dependencies

- `streamlit`: Main framework for creating the web app.
- `requests`: To make HTTP requests for fetching GTFS data.
- `google.transit`: For handling GTFS protobuf messages.
- `folium`: For creating interactive maps to visualize vehicle positions.
- `pandas`: For data manipulation and processing.
- `google.oauth2`: For Google Cloud Platform authentication.
- `google.cloud.storage`: To interact with Google Cloud Storage for file management.
- `streamlit_folium`: For rendering Folium maps in Streamlit.
- `openpyxl`: For downloading data as Excel files in XLSX format.

## License

This project is licensed under the MIT License.

## Acknowledgements

- [Streamlit](https://streamlit.io)
- [Google Transit](https://developers.google.com/transit)
- [Folium](https://python-visualization.github.io/folium/)
- [Google API Client](https://github.com/googleapis/google-api-python-client)
