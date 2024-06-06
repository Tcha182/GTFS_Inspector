# GTFS Inspector

GTFS Inspector is a Streamlit app designed for the teams of RATP Dev to inspect the content of GTFS RT for their subsidiaries. The app allows users to fetch and visualize real-time vehicle and trip data from GTFS RT feeds.

## Features

- Fetch GTFS RT data for vehicle positions and trip updates.
- Visualize vehicle positions on an interactive map.
- Filter data by vehicle IDs.
- Download the fetched data in JSON format.
- Manage GTFS RT URLs (Add, Modify, Delete).

## Installation

1. **Clone the repository:**

    ```bash
    git clone https://github.com/yourusername/gtfs-detective.git
    cd gtfs-detective
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
    - Filter the data by vehicle IDs and view vehicle positions on the map.
    - Download the fetched data in JSON format.

## Functions

- `upload_file(file_name, content, mime_type='application/json', folder_id=None)`: Uploads a file to Google Drive.
- `download_file(file_id)`: Downloads a file from Google Drive.
- `find_file_by_name(file_name)`: Finds a file by name on Google Drive.
- `open_gtfs_realtime_from_url(url)`: Opens GTFS real-time data from a URL.
- `create_map(dataframe, selected_vehicle_ids=None)`: Creates a map with vehicle positions.
- `flatten_dict(d, parent_key='', sep='_')`: Flattens a nested dictionary.
- `protobuf_to_dataframe(feed)`: Converts protobuf messages to a DataFrame.

## Dependencies

- streamlit
- requests
- google.transit
- folium
- pandas
- google.oauth2
- googleapiclient
- streamlit_folium

## License

This project is licensed under the MIT License.

## Acknowledgements

- [Streamlit](https://streamlit.io)
- [Google Transit](https://developers.google.com/transit)
- [Folium](https://python-visualization.github.io/folium/)
- [Google API Client](https://github.com/googleapis/google-api-python-client)
