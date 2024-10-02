import streamlit as st
import requests
from google.transit import gtfs_realtime_pb2
import folium
from folium.plugins import MarkerCluster
from google.protobuf.json_format import MessageToDict
import json
from streamlit_folium import folium_static
import pandas as pd
import io
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import storage

# Set initial page config
st.set_page_config(page_title="GTFS Inspector", layout="wide", page_icon=":bus:")

map_size = 500

# GCS Configuration
@st.cache_resource
def get_gcs_client():
    credentials_info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return storage.Client(credentials=credentials)

gcs_client = get_gcs_client()
bucket_name = st.secrets["bucket"]["gcs_bucket_name"]
bucket = gcs_client.bucket(bucket_name)

# Function to upload or update a network file in GCS
def upload_or_update_network_file(network_name, content):
    blob = bucket.blob(f"{network_name}.json")
    try:
        blob.upload_from_string(content, content_type='application/json')
    except Exception as e:
        st.error(f"Error uploading network file '{network_name}': {e}")

# Function to download a network file from GCS
def download_network_file(network_name):
    blob = bucket.blob(f"{network_name}.json")
    try:
        if not blob.exists():
            return None
        return blob.download_as_text()
    except Exception as e:
        st.error(f"Error downloading network file '{network_name}': {e}")
        return None

# Function to delete a network file from GCS
def delete_network_file(network_name):
    blob = bucket.blob(f"{network_name}.json")
    try:
        if blob.exists():
            blob.delete()
            return True
        else:
            st.error(f"Network file '{network_name}' not found.")
            return False
    except Exception as e:
        st.error(f"Error deleting network file '{network_name}': {e}")
        return False

# Function to list network files in GCS
def list_network_files():
    try:
        return [blob.name.replace('.json', '') for blob in bucket.list_blobs() if blob.name.endswith('.json')]
    except Exception as e:
        st.error(f"Error listing network files: {e}")
        return []

# Load network list into session state
def load_network_list():
    if 'network_list' not in st.session_state:
        st.session_state['network_list'] = list_network_files()
    return st.session_state['network_list']

def clear_network_session_state():
    if 'network_list' in st.session_state:
        del st.session_state['network_list']
    st.rerun()

# Function to open GTFS real-time data from URL
def open_gtfs_realtime_from_url(url):
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(url)
        response.raise_for_status()
        feed.ParseFromString(response.content)
        return feed
    except Exception as e:
        st.error(f"Error opening GTFS RT URL: {e}")
        return None

# Function to cache filtered vehicle data
@st.cache_data
def get_filtered_vehicle_data(vehicle_data, selected_vehicle_ids=None):
    dataframe = vehicle_data.dropna(subset=['vehicle_position_latitude', 'vehicle_position_longitude'])
    if selected_vehicle_ids:
        return dataframe[dataframe['vehicle_vehicle_id'].isin(selected_vehicle_ids)]
    return dataframe

# Function to create a map (no caching here due to folium's limitations)
def create_map(filtered_vehicle_data):
    if not filtered_vehicle_data.empty:
        map_center = [filtered_vehicle_data['vehicle_position_latitude'].mean(), filtered_vehicle_data['vehicle_position_longitude'].mean()]
        zoom_level = 12
    else:
        map_center = [48.8566, 2.3522]
        zoom_level = 12

    m = folium.Map(location=map_center, zoom_start=zoom_level)
    marker_cluster = MarkerCluster().add_to(m)
    positions = []

    for _, row in filtered_vehicle_data.iterrows():
        position = [row['vehicle_position_latitude'], row['vehicle_position_longitude']]
        positions.append(position)
        vehicle_timestamp = int(row['vehicle_timestamp'])
        formatted_time = datetime.utcfromtimestamp(vehicle_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        popup_content = '<br>'.join([
            f"<b>{col.replace('vehicle_', '')}</b>: {formatted_time if col == 'vehicle_timestamp' else row[col]}"
            for col in filtered_vehicle_data.columns if col != 'original_json'
        ])
        popup_content = popup_content.replace('vehicle_vehicle_id', 'vehicle_id')
        folium.Marker(location=position, popup=folium.Popup(popup_content, max_width=300)).add_to(marker_cluster)

    if positions:
        m.fit_bounds(positions)
    return m

# Function to flatten nested dictionaries (protobuf)
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# Function to convert GTFS protobuf feed to dataframe
def protobuf_to_dataframe(feed):
    rows = []
    for entity in feed.entity:
        entity_dict = MessageToDict(entity)
        flattened_entity = flatten_dict(entity_dict)
        flattened_entity['original_json'] = json.dumps(entity_dict)
        rows.append(flattened_entity)
    return pd.DataFrame(rows)

# Title
title_container = st.empty()
title_container.title("GTFS Detective")

# Sidebar
st.sidebar.header("Manage GTFS RT")

# Refresh Button
if st.sidebar.button(":material/refresh: Refresh"):
    clear_network_session_state()

action = st.sidebar.selectbox("Action", ["Add", "Modify", "Delete"], key="action")

# Load network list
network_list = load_network_list()

# Action: Add
if action == "Add":
    with st.sidebar.form("Add Network"):
        name = st.text_input("Network Name")
        vehicle_positions_url = st.text_input("Vehicle Positions URL")
        trip_updates_url = st.text_input("Trip Updates URL")
        submitted = st.form_submit_button("Add to GTFS-RT list")
        if submitted and name:
            network_data = {
                "vehicle_positions_url": vehicle_positions_url,
                "trip_updates_url": trip_updates_url
            }
            upload_or_update_network_file(name, json.dumps(network_data, indent=4))
            clear_network_session_state()

# Action: Modify
if action == "Modify":
    name_to_modify = st.sidebar.selectbox("Select GTFS RT to Modify", network_list)
    if name_to_modify:
        network_data = json.loads(download_network_file(name_to_modify))
        if network_data:
            with st.sidebar.form("Modify Network"):
                vehicle_positions_url = st.text_input("Vehicle Positions URL", network_data.get("vehicle_positions_url", ""))
                trip_updates_url = st.text_input("Trip Updates URL", network_data.get("trip_updates_url", ""))
                submitted = st.form_submit_button("Save changes")
                if submitted:
                    network_data = {
                        "vehicle_positions_url": vehicle_positions_url,
                        "trip_updates_url": trip_updates_url
                    }
                    upload_or_update_network_file(name_to_modify, json.dumps(network_data, indent=4))
                    clear_network_session_state()

# Action: Delete
if action == "Delete":
    name_to_delete = st.sidebar.selectbox("Select GTFS RT to Delete", network_list)
    if name_to_delete:
        if st.sidebar.button("Delete Selected GTFS RT"):
            delete_network_file(name_to_delete)
            clear_network_session_state()

# Select GTFS RT
network_list = load_network_list()
if network_list:
    selected_name = st.selectbox("Select GTFS RT", network_list)
else:
    st.write("No networks available. Please add a network.")

# Load GTFS data button action
if st.button("Load GTFS", use_container_width=True):
    if selected_name:
        status_container = st.empty()
        status_container.info("Loading data...")
        try:
            network_data = json.loads(download_network_file(selected_name))
            if not network_data:
                status_container.error(f"No data found for network '{selected_name}'.")
            else:
                vehicle_data, trip_data = pd.DataFrame(), pd.DataFrame()

                if network_data.get("vehicle_positions_url"):
                    vehicle_feed = open_gtfs_realtime_from_url(network_data["vehicle_positions_url"])
                    if vehicle_feed:
                        vehicle_data = protobuf_to_dataframe(vehicle_feed)
                        status_container.write(f"Vehicle data - Length: {len(vehicle_data)}")
                if network_data.get("trip_updates_url"):
                    trip_feed = open_gtfs_realtime_from_url(network_data["trip_updates_url"])
                    if trip_feed:
                        trip_data = protobuf_to_dataframe(trip_feed)
                        status_container.write(f"Trip updates - Length: {len(trip_data)}")

                fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state['vehicle_data'] = vehicle_data
                st.session_state['trip_data'] = trip_data
                st.session_state['selected_name'] = selected_name
                st.session_state['fetch_time'] = fetch_time

                title_container.title(f"GTFS Detective - {selected_name} - {fetch_time}")

                status_container.success("Data loaded successfully!")
        except Exception as e:
            status_container.error(f"Error loading data for {selected_name}: {e}")

# Display GTFS data
if 'vehicle_data' in st.session_state and 'trip_data' in st.session_state and 'fetch_time' in st.session_state:
    vehicle_data = st.session_state['vehicle_data']
    trip_data = st.session_state['trip_data']
    selected_name = st.session_state['selected_name']
    fetch_time = st.session_state['fetch_time']

    if not vehicle_data.empty or not trip_data.empty:
        col1, col2 = st.columns(2)
        available_vehicle_ids = set()
        if 'vehicle_vehicle_id' in vehicle_data.columns:
            available_vehicle_ids.update(vehicle_data['vehicle_vehicle_id'].unique())
        if 'tripUpdate_vehicle_id' in trip_data.columns:
            available_vehicle_ids.update(trip_data['tripUpdate_vehicle_id'].unique())

        sorted_vehicle_ids = sorted(available_vehicle_ids, key=lambda x: int(x) if str(x).isdigit() else str(x))
        selected_vehicle_ids = col1.multiselect("Select Vehicles", sorted_vehicle_ids)

        # Get filtered vehicle data using cache
        filtered_vehicle_data = get_filtered_vehicle_data(vehicle_data, selected_vehicle_ids)

        # Prepare the data for tabs
        tabs = []
        tab_contents = []

        if not filtered_vehicle_data.empty:
            tabs.append(f"Vehicle Positions ({len(filtered_vehicle_data)})")
            tab_contents.append(filtered_vehicle_data)
        if not trip_data.empty:
            tabs.append(f"Trip Updates ({len(trip_data)})")
            tab_contents.append(trip_data)

        # Display tabs for data
        if tabs:
            selected_tab = col1.tabs(tabs)

            for idx, tab in enumerate(selected_tab):
                with tab:
                    data = tab_contents[idx]
                    json_list = [json.loads(item) for item in data['original_json']]
                    json_str = json.dumps(json_list, indent=4)
                    data_type = "Vehicle_Positions" if "vehicle_position_latitude" in data.columns else "Trip_Updates"
                    file_name = f"{selected_name}_{data_type}{'_Filtered' if selected_vehicle_ids else ''}_{fetch_time}.json".replace(" ", "_").replace(":", "-")
                    st.download_button(label=f"Download {data_type.replace('_', ' ')} JSON", data=json_str, file_name=file_name, mime="application/json")
                    st.json(json_list)

        # Display map in second column if there are vehicle positions
        if not filtered_vehicle_data.empty and 'vehicle_position_latitude' in filtered_vehicle_data.columns and 'vehicle_position_longitude' in filtered_vehicle_data.columns:
            map_obj = create_map(filtered_vehicle_data)
            with col2:
                folium_static(map_obj, width=map_size, height=map_size)
        else:
            col2.write("No vehicle positions available to display on the map.")
