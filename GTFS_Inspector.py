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
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# Set initial page config
st.set_page_config(page_title="GTFS Detective", layout="wide", page_icon=":bus:")

# Load the credentials from Streamlit secrets
credentials_info = st.secrets["gcp_service_account"]

# Create a credentials object from the secrets
credentials = service_account.Credentials.from_service_account_info(credentials_info)

# Use the credentials to build the Google Drive service
service = build('drive', 'v3', credentials=credentials)

# Function to upload a file
def upload_file(file_name, content, mime_type='application/json', folder_id=None):
    file_metadata = {'name': file_name}
    if folder_id:
        file_metadata['parents'] = [folder_id]
    media = MediaIoBaseUpload(io.BytesIO(content.encode()), mimetype=mime_type)
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

# Function to download a file
def download_file(file_id):
    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    file.seek(0)
    return file.read().decode()

# Function to find a file by name
def find_file_by_name(file_name):
    results = service.files().list(q=f"name='{file_name}'", fields="files(id, name)").execute()
    items = results.get('files', [])
    return items[0]['id'] if items else None

# Your Google Drive file name for the gtfs_rt_urls.json
URL_PAIRS_FILE_NAME = 'gtfs_rt_urls.json'

# Find the file ID
URL_PAIRS_FILE_ID = find_file_by_name(URL_PAIRS_FILE_NAME)

# Read URL pairs from Google Drive or create the file if it doesn't exist
if URL_PAIRS_FILE_ID:
    url_pairs = json.loads(download_file(URL_PAIRS_FILE_ID))
else:
    url_pairs = {}
    URL_PAIRS_FILE_ID = upload_file(URL_PAIRS_FILE_NAME, json.dumps(url_pairs, indent=4))

def open_gtfs_realtime_from_url(url):
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url)
    response.raise_for_status()
    feed.ParseFromString(response.content)
    return feed

def create_map(dataframe, selected_vehicle_ids=None):
    dataframe = dataframe.dropna(subset=['vehicle_position_latitude', 'vehicle_position_longitude'])
    if not dataframe.empty:
        map_center = [dataframe['vehicle_position_latitude'].mean(), dataframe['vehicle_position_longitude'].mean()]
        zoom_level = 12
    else:
        map_center = [48.8566, 2.3522]
        zoom_level = 12

    m = folium.Map(location=map_center, zoom_start=zoom_level)
    marker_cluster = MarkerCluster().add_to(m)
    positions = []

    for _, row in dataframe.iterrows():
        if selected_vehicle_ids is None or row['vehicle_vehicle_id'] in selected_vehicle_ids:
            position = [row['vehicle_position_latitude'], row['vehicle_position_longitude']]
            positions.append(position)
            vehicle_timestamp = int(row['vehicle_timestamp'])
            formatted_time = datetime.utcfromtimestamp(vehicle_timestamp).strftime('%Y-%m-%d %H:%M:%S')
            popup_content = '<br>'.join([f"<b>{col.replace('vehicle_', '')}</b>: {formatted_time if col == 'vehicle_timestamp' else row[col]}" for col in dataframe.columns if col != 'original_json'])
            popup_content = popup_content.replace('vehicle_vehicle_id', 'vehicle_id')
            folium.Marker(location=position, popup=folium.Popup(popup_content, max_width=300)).add_to(marker_cluster)
    if positions:
        m.fit_bounds(positions)
    return m

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def protobuf_to_dataframe(feed):
    rows = []
    for entity in feed.entity:
        entity_dict = MessageToDict(entity)
        flattened_entity = flatten_dict(entity_dict)
        flattened_entity['original_json'] = json.dumps(entity_dict)
        rows.append(flattened_entity)
    return pd.DataFrame(rows)

title_container = st.empty()
title_container.title("GTFS Detective")

st.sidebar.header("Manage GTFS RT")
action = st.sidebar.selectbox("Action", ["Add", "Modify", "Delete"])

if action == "Add":
    with st.sidebar.form("Add Name"):
        name = st.text_input("Name")
        vehicle_positions_url = st.text_input("Vehicle Positions URL")
        trip_updates_url = st.text_input("Trip Updates URL")
        submitted = st.form_submit_button("Add to GTFS-RT list")
        if submitted and name:
            url_pairs[name] = {
                "vehicle_positions_url": vehicle_positions_url,
                "trip_updates_url": trip_updates_url
            }
            upload_file(URL_PAIRS_FILE_NAME, json.dumps(url_pairs, indent=4))
            st.experimental_rerun()

if action == "Modify":
    name_to_modify = st.sidebar.selectbox("Select GTFS RT to Modify", list(url_pairs.keys()))
    if name_to_modify:
        with st.sidebar.form("Modify"):
            vehicle_positions_url = st.text_input("Vehicle Positions URL", url_pairs[name_to_modify]["vehicle_positions_url"])
            trip_updates_url = st.text_input("Trip Updates URL", url_pairs[name_to_modify]["trip_updates_url"])
            submitted = st.form_submit_button("Save changes")
            if submitted:
                url_pairs[name_to_modify] = {
                    "vehicle_positions_url": vehicle_positions_url,
                    "trip_updates_url": trip_updates_url
                }
                upload_file(URL_PAIRS_FILE_NAME, json.dumps(url_pairs, indent=4))
                st.experimental_rerun()

if action == "Delete":
    name_to_delete = st.sidebar.selectbox("Select Name to Delete", list(url_pairs.keys()))
    if name_to_delete:
        if st.sidebar.button("Delete Selected GTFS RT"):
            del url_pairs[name_to_delete]
            upload_file(URL_PAIRS_FILE_NAME, json.dumps(url_pairs, indent=4))
            st.experimental_rerun()

selected_name = st.selectbox("Select GTFS RT to Fetch", list(url_pairs.keys()))

if st.button("Fetch GTFS", use_container_width=True):
    if selected_name:
        status_container = st.status("Fetching data...", state="running")
        try:
            urls = url_pairs[selected_name]
            vehicle_data, trip_data = pd.DataFrame(), pd.DataFrame()
            
            if urls["vehicle_positions_url"]:
                vehicle_feed = open_gtfs_realtime_from_url(urls["vehicle_positions_url"])
                vehicle_data = protobuf_to_dataframe(vehicle_feed)
                status_container.write(f"Vehicle data - Length: {len(vehicle_data)}")
            if urls["trip_updates_url"]:
                trip_feed = open_gtfs_realtime_from_url(urls["trip_updates_url"])
                trip_data = protobuf_to_dataframe(trip_feed)
                status_container.write(f"Trip updates - Length: {len(trip_data)}")

            fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state['vehicle_data'] = vehicle_data
            st.session_state['trip_data'] = trip_data
            st.session_state['selected_name'] = selected_name
            st.session_state['fetch_time'] = fetch_time
            
            title_container.title(f"GTFS Inspector - {selected_name} - {fetch_time}")

            status_container.update(label=f"Data loaded successfully!", state="complete")
        except Exception as e:
            status_container.update(label=f"Error fetching data for {selected_name}: {e}", state="error")

if 'vehicle_data' in st.session_state and 'trip_data' in st.session_state and 'fetch_time' in st.session_state:
    vehicle_data = st.session_state['vehicle_data']
    trip_data = st.session_state['trip_data']
    selected_name = st.session_state['selected_name']
    fetch_time = st.session_state['fetch_time']
    
    if not vehicle_data.empty or not trip_data.empty:
        col1, col2 = st.columns(2)
        available_vehicle_ids = set(vehicle_data['vehicle_vehicle_id'].unique()) if 'vehicle_vehicle_id' in vehicle_data.columns else set()
        available_vehicle_ids.update(trip_data['tripUpdate_vehicle_id'].unique() if 'tripUpdate_vehicle_id' in trip_data.columns else [])
        sorted_vehicle_ids = sorted(available_vehicle_ids, key=lambda x: int(x) if x.isdigit() else x)
        selected_vehicle_ids = col1.multiselect("Select Vehicles", sorted_vehicle_ids)
        
        if selected_vehicle_ids:
            filtered_vehicle_data = vehicle_data[vehicle_data['vehicle_vehicle_id'].isin(selected_vehicle_ids)] if 'vehicle_vehicle_id' in vehicle_data.columns else pd.DataFrame()
            filtered_trip_data = trip_data[trip_data['tripUpdate_vehicle_id'].isin(selected_vehicle_ids)] if 'tripUpdate_vehicle_id' in trip_data.columns else pd.DataFrame()
        else:
            filtered_vehicle_data = vehicle_data
            filtered_trip_data = trip_data
        
        tabs = []
        if not filtered_vehicle_data.empty:
            tabs.append(f"Vehicle Positions ({len(filtered_vehicle_data)})")
        if not filtered_trip_data.empty:
            tabs.append(f"Trip Updates ({len(filtered_trip_data)})")

        if tabs:
            selected_tab = col1.tabs(tabs)
            
            if not filtered_vehicle_data.empty:
                with selected_tab[0]:
                    vehicle_json_list = [json.loads(item) for item in filtered_vehicle_data['original_json']]
                    vehicle_json_str = json.dumps(vehicle_json_list, indent=4)
                    file_name = f"{selected_name}_Vehicle_Positions{'_Filtered' if selected_vehicle_ids else ''}_{fetch_time}.json".replace(" ", "_").replace(":", "-")
                    st.download_button(label="Download Vehicle Positions JSON", data=vehicle_json_str, file_name=file_name, mime="application/json")
                    st.json(vehicle_json_list)
                    
            if not filtered_trip_data.empty:
                with selected_tab[1]:
                    trip_json_list = [json.loads(item) for item in filtered_trip_data['original_json']]
                    trip_json_str = json.dumps(trip_json_list, indent=4)
                    file_name = f"{selected_name}_Trip_Updates{'_Filtered' if selected_vehicle_ids else ''}_{fetch_time}.json".replace(" ", "_").replace(":", "-")
                    st.download_button(label="Download Trip Updates JSON", data=trip_json_str, file_name=file_name, mime="application/json")
                    st.json(trip_json_list)

        if not filtered_vehicle_data.empty and 'vehicle_position_latitude' in filtered_vehicle_data.columns and 'vehicle_position_longitude' in filtered_vehicle_data.columns:
            map = create_map(filtered_vehicle_data, selected_vehicle_ids if selected_vehicle_ids else None)
            with col2:
                folium_static(map, 800, 800)
        else:
            col2.write("No vehicle positions available to display on the map.")
