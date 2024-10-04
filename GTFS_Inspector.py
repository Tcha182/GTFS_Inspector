import streamlit as st
import requests
from google.transit import gtfs_realtime_pb2
import folium
from folium.plugins import MarkerCluster
from google.protobuf.json_format import MessageToDict
import json
from streamlit_folium import folium_static
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import storage
import io

# Set initial page config
st.set_page_config(page_title="GTFS Inspector", layout="wide", page_icon=":bus:")

st.markdown(
    """
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .css-1v0mbdj {padding-top: 0rem;}
    </style>
    """,
    unsafe_allow_html=True
)

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

# Function to get filtered data
@st.cache_data
def get_filtered_data(vehicle_data, trip_data, filter_option, selected_value):
    # Create copies to avoid modifying the original data
    filtered_vehicle_data = vehicle_data.copy()
    filtered_trip_data = trip_data.copy()

    if filter_option == "Vehicle ID":
        # Filter by vehicle ID
        vehicle_id = selected_value
        if vehicle_id:
            if 'vehicle_vehicle_id' in filtered_vehicle_data.columns:
                filtered_vehicle_data = filtered_vehicle_data[filtered_vehicle_data['vehicle_vehicle_id'] == vehicle_id]
            if 'tripUpdate_vehicle_id' in filtered_trip_data.columns:
                filtered_trip_data = filtered_trip_data[filtered_trip_data['tripUpdate_vehicle_id'] == vehicle_id]

    elif filter_option == "Trip ID":
        # Filter by trip ID
        trip_id = selected_value
        if trip_id:
            if 'tripUpdate_trip_tripId' in filtered_trip_data.columns:
                filtered_trip_data = filtered_trip_data[filtered_trip_data['tripUpdate_trip_tripId'] == trip_id]
            # Get vehicle IDs associated with the trip from trip updates
            vehicle_ids = filtered_trip_data['tripUpdate_vehicle_id'].dropna().unique().tolist() if 'tripUpdate_vehicle_id' in filtered_trip_data.columns else []
            # Also include vehicle data for these vehicles
            if vehicle_ids and 'vehicle_vehicle_id' in filtered_vehicle_data.columns:
                filtered_vehicle_data = filtered_vehicle_data[filtered_vehicle_data['vehicle_vehicle_id'].isin(vehicle_ids)]
            else:
                # If vehicle IDs not found, try matching trip IDs in vehicle data
                if 'vehicle_trip_tripId' in filtered_vehicle_data.columns:
                    filtered_vehicle_data = filtered_vehicle_data[filtered_vehicle_data['vehicle_trip_tripId'] == trip_id]

    elif filter_option == "Route ID":
        # Filter by route ID
        route_id = selected_value
        if route_id:
            if 'vehicle_trip_routeId' in filtered_vehicle_data.columns:
                filtered_vehicle_data = filtered_vehicle_data[filtered_vehicle_data['vehicle_trip_routeId'] == route_id]
            if 'tripUpdate_trip_routeId' in filtered_trip_data.columns:
                filtered_trip_data = filtered_trip_data[filtered_trip_data['tripUpdate_trip_routeId'] == route_id]

    return filtered_vehicle_data, filtered_trip_data

# Title
title_container = st.empty()
if 'title' not in st.session_state:
    st.session_state['title'] = "GTFS Inspector"

title_container.title(st.session_state.title)

# Sidebar and other UI components
st.sidebar.header("Manage Sources")

# Refresh Button
if st.sidebar.button(":material/refresh: Refresh"):
    clear_network_session_state()

action = st.sidebar.selectbox("Action", ["Add", "Modify", "Delete"], key="action")

# Load network list
network_list = load_network_list()

# Action: Add
if action == "Add":
    with st.sidebar.form("Add Source"):
        name = st.text_input("Network Name")
        vehicle_positions_url = st.text_input("Vehicle Positions URL")
        trip_updates_url = st.text_input("Trip Updates URL")
        submitted = st.form_submit_button(":material/add: Add to GTFS-RT list")
        if submitted and name:
            network_data = {
                "vehicle_positions_url": vehicle_positions_url,
                "trip_updates_url": trip_updates_url
            }
            upload_or_update_network_file(name, json.dumps(network_data, indent=4))
            clear_network_session_state()

# Action: Modify
if action == "Modify":
    name_to_modify = st.sidebar.selectbox("Select Source to Modify", network_list)
    if name_to_modify:
        network_data = json.loads(download_network_file(name_to_modify))
        if network_data:
            with st.sidebar.form("Modify Network"):
                vehicle_positions_url = st.text_input("Vehicle Positions URL", network_data.get("vehicle_positions_url", ""))
                trip_updates_url = st.text_input("Trip Updates URL", network_data.get("trip_updates_url", ""))
                submitted = st.form_submit_button(":material/save: Save changes")
                if submitted:
                    network_data = {
                        "vehicle_positions_url": vehicle_positions_url,
                        "trip_updates_url": trip_updates_url
                    }
                    upload_or_update_network_file(name_to_modify, json.dumps(network_data, indent=4))
                    clear_network_session_state()

# Action: Delete
if action == "Delete":
    name_to_delete = st.sidebar.selectbox("Select Source to Delete", network_list)
    if name_to_delete:
        if st.sidebar.button(f":material/delete: Delete {name_to_delete}"):
            delete_network_file(name_to_delete)
            clear_network_session_state()

# Select GTFS RT
network_list = load_network_list()
if network_list:
    selected_name = st.selectbox("Select Source", network_list)
else:
    st.write("No networks available. Please add a network.")

# Load GTFS data button action
if st.button(f":material/system_update_alt: Load GTFS RT {selected_name}", use_container_width=True):
    if selected_name:
        st.toast(":blue[:material/download:] Loading data...")
        try:
            network_data = json.loads(download_network_file(selected_name))
            if not network_data:
                st.toast(f":orange[:material/error:] No data found for network '{selected_name}'.")
            else:
                vehicle_data, trip_data = pd.DataFrame(), pd.DataFrame()

                if network_data.get("vehicle_positions_url"):
                    vehicle_feed = open_gtfs_realtime_from_url(network_data["vehicle_positions_url"])
                    if vehicle_feed:
                        vehicle_data = protobuf_to_dataframe(vehicle_feed)
                        st.toast(f":material/list: Vehicle data - Length: {len(vehicle_data)}")
                if network_data.get("trip_updates_url"):
                    trip_feed = open_gtfs_realtime_from_url(network_data["trip_updates_url"])
                    if trip_feed:
                        trip_data = protobuf_to_dataframe(trip_feed)
                        st.toast(f":material/list: Trip updates - Length: {len(trip_data)}")

                fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state['vehicle_data'] = vehicle_data
                st.session_state['trip_data'] = trip_data
                st.session_state['selected_name'] = selected_name
                st.session_state['fetch_time'] = fetch_time
                st.session_state['title'] = f"{selected_name} - {fetch_time}"

                title_container.title(st.session_state.title)

                st.toast(":green[:material/check_circle:]  Data loaded successfully!")
        except Exception as e:
            st.toast(f":orange[:material/error:] Error loading data for {selected_name}: {e}")

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
        selected_value = None

        if filter_option == "Vehicle ID":
            # Get available vehicle IDs to filter
            available_vehicle_ids = set()
            if 'vehicle_vehicle_id' in vehicle_data.columns:
                available_vehicle_ids.update(vehicle_data['vehicle_vehicle_id'].dropna().unique())
            sorted_vehicle_ids = smart_sort(available_vehicle_ids)
            selected_vehicle_id = st.selectbox("Select Vehicle", [""] + sorted_vehicle_ids)
            if selected_vehicle_id == "":
                selected_vehicle_id = None
            selected_value = selected_vehicle_id
        elif filter_option == "Trip ID":
            # Trip ID filter
            available_trip_ids = set()
            if 'vehicle_trip_tripId' in vehicle_data.columns:
                available_trip_ids.update(vehicle_data['vehicle_trip_tripId'].dropna().unique())
            if 'tripUpdate_trip_tripId' in trip_data.columns:
                available_trip_ids.update(trip_data['tripUpdate_trip_tripId'].dropna().unique())
            sorted_trip_ids = smart_sort(available_trip_ids)
            selected_trip_id = st.selectbox("Select Trip", [""] + sorted_trip_ids)
            if selected_trip_id == "":
                selected_trip_id = None
            selected_value = selected_trip_id

        elif filter_option == "Route ID":
            # Route ID filter
            available_route_ids = set()
            if 'vehicle_trip_routeId' in vehicle_data.columns:
                available_route_ids.update(vehicle_data['vehicle_trip_routeId'].dropna().unique())
            if 'tripUpdate_trip_routeId' in trip_data.columns:
                available_route_ids.update(trip_data['tripUpdate_trip_routeId'].dropna().unique())
            sorted_route_ids = smart_sort(available_route_ids)
            selected_route_id = st.selectbox("Select Route", [""] + sorted_route_ids)
            if selected_route_id == "":
                selected_route_id = None
            selected_value = selected_route_id

        # Get filtered vehicle data and trip data using the selected filters
        filtered_vehicle_data, filtered_trip_data = get_filtered_data(
            vehicle_data,
            trip_data,
            filter_option=filter_option,
            selected_value=selected_value
        )

        # Display the filtered data in tabs
        if not filtered_vehicle_data.empty or not filtered_trip_data.empty:
            col1, col2 = st.columns(2)

            tabs = []
            tab_contents = []

            # Display map in the second column if vehicle data contains location information
            if not filtered_vehicle_data.empty and 'vehicle_position_latitude' in filtered_vehicle_data.columns and 'vehicle_position_longitude' in filtered_vehicle_data.columns:
                map_obj = create_map(filtered_vehicle_data)
                with col2:
                    st.write("Map View:")
                    folium_static(map_obj, width=map_size, height=map_size)
            else:
                col2.write("No vehicle positions available to display on the map.")

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
                        file_name_base = f"{selected_name}_{data_type}{'_Filtered' if selected_value else ''}_{fetch_time}".replace(" ", "_").replace(":", "-")

                        # Provide download buttons for JSON and XLSX data
                        st.download_button(label=f":material/download::material/data_object: Download {data_type.replace('_', ' ')} JSON", data=json_str, file_name=f"{file_name_base}.json", mime="application/json")
                        towrite = io.BytesIO()
                        data.to_excel(towrite, index=False, header=True)
                        towrite.seek(0)
                        st.download_button(label=f":material/download::material/table: Download {data_type.replace('_', ' ')} XLSX", data=towrite, file_name=f"{file_name_base}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                        # Show JSON data in a formatted manner
                        st.json(json_list)

        else:
            st.write("No data available for the selected filters.")
