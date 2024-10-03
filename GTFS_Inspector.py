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

@st.cache_data
def get_filtered_data(vehicle_data, trip_data, selected_vehicle_ids=None, selected_trip_ids=None, selected_route_ids=None):
    # Filter vehicle_data
    if not vehicle_data.empty:
        # Filter by vehicle_id
        if selected_vehicle_ids and 'vehicle_vehicle_id' in vehicle_data.columns:
            vehicle_data = vehicle_data[vehicle_data['vehicle_vehicle_id'].isin(selected_vehicle_ids)]

        # Filter by trip_id
        if selected_trip_ids and 'vehicle_trip_tripId' in vehicle_data.columns:
            vehicle_data = vehicle_data[vehicle_data['vehicle_trip_tripId'].isin(selected_trip_ids)]

        # Filter by route_id
        if selected_route_ids and 'vehicle_trip_routeId' in vehicle_data.columns:
            vehicle_data = vehicle_data[vehicle_data['vehicle_trip_routeId'].isin(selected_route_ids)]

        # Collect trip IDs from vehicle_data
        if 'vehicle_trip_tripId' in vehicle_data.columns:
            vehicle_trip_ids = vehicle_data['vehicle_trip_tripId'].dropna().unique().tolist()
        else:
            vehicle_trip_ids = []
    else:
        vehicle_trip_ids = []

    # Filter trip_data
    if not trip_data.empty:
        # Filter by trip_id
        if selected_trip_ids and 'tripUpdate_trip_tripId' in trip_data.columns:
            trip_data = trip_data[trip_data['tripUpdate_trip_tripId'].isin(selected_trip_ids)]

        # Filter by route_id
        if selected_route_ids and 'tripUpdate_trip_routeId' in trip_data.columns:
            trip_data = trip_data[trip_data['tripUpdate_trip_routeId'].isin(selected_route_ids)]

        # Filter by vehicle_id
        if selected_vehicle_ids and 'tripUpdate_vehicle_id' in trip_data.columns:
            trip_data = trip_data[trip_data['tripUpdate_vehicle_id'].isin(selected_vehicle_ids)]

        # Include trips associated with vehicles in vehicle_data
        if vehicle_trip_ids and 'tripUpdate_trip_tripId' in trip_data.columns:
            trip_data = trip_data[trip_data['tripUpdate_trip_tripId'].isin(vehicle_trip_ids)]

        # Collect vehicle IDs from trip_data
        if 'tripUpdate_vehicle_id' in trip_data.columns:
            trip_vehicle_ids = trip_data['tripUpdate_vehicle_id'].dropna().unique().tolist()
        else:
            trip_vehicle_ids = []
    else:
        trip_vehicle_ids = []

    # Filter vehicle_data based on vehicle IDs from trip_data
    if not vehicle_data.empty and trip_vehicle_ids:
        if 'vehicle_vehicle_id' in vehicle_data.columns:
            vehicle_data = vehicle_data[vehicle_data['vehicle_vehicle_id'].isin(trip_vehicle_ids)]

    return vehicle_data, trip_data



# Function to create a map
@st.cache_resource
def create_map(filtered_vehicle_data):
    # Drop rows with missing latitude and longitude values
    if 'vehicle_position_latitude' in filtered_vehicle_data.columns and 'vehicle_position_longitude' in filtered_vehicle_data.columns:
        filtered_vehicle_data = filtered_vehicle_data.dropna(subset=['vehicle_position_latitude', 'vehicle_position_longitude'])

    if not filtered_vehicle_data.empty:
        map_center = [filtered_vehicle_data['vehicle_position_latitude'].mean(), filtered_vehicle_data['vehicle_position_longitude'].mean()]
        zoom_level = 12
    else:
        map_center = [48.8566, 2.3522]  # Default location (e.g., Paris)
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

def smart_sort(data):
    if all(str(x).isdigit() for x in data):
        # If all items are numeric, sort as integers
        return sorted(data, key=int)
    else:
        # If items are mixed, sort as strings
        return sorted(data, key=str)

# Title
title_container = st.empty()
if 'title' not in st.session_state:
    st.session_state['title'] = "GTFS Inspector"

title_container.title(st.session_state.title)

# Sidebar
st.sidebar.header("Manage Sources")

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
        if st.sidebar.button(f"Delete {name_to_delete}"):
            delete_network_file(name_to_delete)
            clear_network_session_state()

# Select GTFS RT
network_list = load_network_list()
if network_list:
    selected_name = st.selectbox("Select GTFS RT", network_list)
else:
    st.write("No networks available. Please add a network.")

# Load GTFS data button action
if st.button(f":material/system_update_alt: Load GTFS RT {selected_name}", use_container_width=True):
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
                st.session_state['title'] = f"{selected_name} - {fetch_time}"

                title_container.title(st.session_state.title)

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

        # Preliminary filter selection
        filter_option = st.selectbox("Select Filter Type", ["Vehicle ID", "Trip ID", "Route ID"], key="filter_option")

        # Define filter variables to None initially
        selected_vehicle_ids, selected_trip_ids, selected_route_ids = None, None, None

        # Depending on the selected filter option, show the appropriate multiselect filter
        if filter_option == "Vehicle ID":
            # Vehicle ID filter
            available_vehicle_ids = set()
            if 'vehicle_vehicle_id' in vehicle_data.columns:
                available_vehicle_ids.update(vehicle_data['vehicle_vehicle_id'].unique())
            sorted_vehicle_ids = smart_sort(available_vehicle_ids)
            selected_vehicle_ids = st.multiselect("Select Vehicles", sorted_vehicle_ids)

        elif filter_option == "Trip ID":
            # Trip ID filter
            available_trip_ids = set()
            if 'vehicle_trip_tripId' in vehicle_data.columns:
                available_trip_ids.update(vehicle_data['vehicle_trip_tripId'].unique())
            if 'tripUpdate_trip_tripId' in trip_data.columns:
                available_trip_ids.update(trip_data['tripUpdate_trip_tripId'].unique())
            sorted_trip_ids = smart_sort(available_trip_ids)
            selected_trip_ids = st.multiselect("Select Trips", sorted_trip_ids)

        elif filter_option == "Route ID":
            # Route ID filter
            available_route_ids = set()
            if 'vehicle_trip_routeId' in vehicle_data.columns:
                available_route_ids.update(vehicle_data['vehicle_trip_routeId'].unique())
            if 'tripUpdate_trip_routeId' in trip_data.columns:
                available_route_ids.update(trip_data['tripUpdate_trip_routeId'].unique())
            sorted_route_ids = smart_sort(available_route_ids)
            selected_route_ids = st.multiselect("Select Routes", sorted_route_ids)

        # Get filtered vehicle data and trip data using the selected filters
        filtered_vehicle_data, filtered_trip_data = get_filtered_data(
            vehicle_data, 
            trip_data, 
            selected_vehicle_ids=selected_vehicle_ids,
            selected_trip_ids=selected_trip_ids,
            selected_route_ids=selected_route_ids
        )

        # Display the filtered data in tabs
        if not filtered_vehicle_data.empty or not filtered_trip_data.empty:
            col1, col2 = st.columns(2)

            tabs = []
            tab_contents = []

            # Add vehicle data tab if available
            if not filtered_vehicle_data.empty:
                tabs.append(f"Vehicle Positions ({len(filtered_vehicle_data)})")
                tab_contents.append(filtered_vehicle_data)
            
            # Add trip data tab if available
            if not filtered_trip_data.empty:
                tabs.append(f"Trip Updates ({len(filtered_trip_data)})")
                tab_contents.append(filtered_trip_data)

            if tabs:
                selected_tabs = col1.tabs(tabs)

                for idx, tab in enumerate(selected_tabs):
                    with tab:
                        data = tab_contents[idx]
                        json_list = [json.loads(item) for item in data['original_json']]
                        json_str = json.dumps(json_list, indent=4)
                        
                        # Determine data type (vehicle or trip)
                        data_type = "Vehicle_Positions" if "vehicle_position_latitude" in data.columns else "Trip_Updates"
                        file_name = f"{selected_name}_{data_type}{'_Filtered' if selected_vehicle_ids or selected_trip_ids or selected_route_ids else ''}_{fetch_time}.json".replace(" ", "_").replace(":", "-")

                        # Provide download button for JSON data
                        st.download_button(label=f"Download {data_type.replace('_', ' ')} JSON", data=json_str, file_name=file_name, mime="application/json")
                        
                        # Show JSON data in a formatted manner
                        st.json(json_list)

            # Display map in the second column if vehicle data contains location information
            if not filtered_vehicle_data.empty and 'vehicle_position_latitude' in filtered_vehicle_data.columns and 'vehicle_position_longitude' in filtered_vehicle_data.columns:
                map_obj = create_map(filtered_vehicle_data)
                with col2:
                    st.write("Map View:")
                    folium_static(map_obj, width=map_size, height=map_size)
            else:
                col2.write("No vehicle positions available to display on the map.")
        else:
            st.write("No data available for the selected filters.")