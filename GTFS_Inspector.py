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

# ────────────────────────────
# Page config & simple CSS
# ────────────────────────────
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

MAP_SIZE = 500  # px

# ────────────────────────────
# Google Cloud Storage helpers
# ────────────────────────────
@st.cache_resource
def get_gcs_client():
    credentials_info = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return storage.Client(credentials=credentials)

gcs_client = get_gcs_client()
bucket_name = st.secrets["bucket"]["gcs_bucket_name"]
bucket = gcs_client.bucket(bucket_name)

def upload_or_update_network_file(network_name: str, content: str):
    """Create or overwrite one JSON definition file in the bucket."""
    blob = bucket.blob(f"{network_name}.json")
    try:
        blob.upload_from_string(content, content_type="application/json")
    except Exception as e:
        st.error(f"Error uploading network file '{network_name}': {e}")

def download_network_file(network_name: str):
    blob = bucket.blob(f"{network_name}.json")
    try:
        return None if not blob.exists() else blob.download_as_text()
    except Exception as e:
        st.error(f"Error downloading network file '{network_name}': {e}")
        return None

def delete_network_file(network_name: str) -> bool:
    blob = bucket.blob(f"{network_name}.json")
    try:
        if blob.exists():
            blob.delete()
            return True
        st.error(f"Network file '{network_name}' not found.")
        return False
    except Exception as e:
        st.error(f"Error deleting network file '{network_name}': {e}")
        return False

def list_network_files():
    try:
        return [
            blob.name.replace(".json", "")
            for blob in bucket.list_blobs()
            if blob.name.endswith(".json")
        ]
    except Exception as e:
        st.error(f"Error listing network files: {e}")
        return []

def load_network_list():
    if "network_list" not in st.session_state:
        st.session_state["network_list"] = list_network_files()
    return st.session_state["network_list"]

def clear_network_session_state():
    if "network_list" in st.session_state:
        del st.session_state["network_list"]
    st.rerun()

# ────────────────────────────
# GTFS-RT utilities
# ────────────────────────────
def open_gtfs_realtime_from_url(url: str):
    """Retrieve & parse a GTFS-RT protobuf feed."""
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        feed.ParseFromString(response.content)
        return feed
    except Exception as e:
        st.error(f"Error opening GTFS-RT URL: {e}")
        return None

def flatten_dict(d: dict, parent_key: str = "", sep: str = "_"):
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
        flattened = flatten_dict(entity_dict)
        flattened["original_json"] = json.dumps(entity_dict, separators=(",", ":"))
        rows.append(flattened)
    return pd.DataFrame(rows)

def smart_sort(values):
    """Safely sort a mix of numeric and non-numeric values."""
    clean_values = [v for v in values if pd.notnull(v)]
    numeric = []
    non_numeric = []
    for v in clean_values:
        s = str(v)
        if s.isdigit():
            numeric.append(int(s))
        else:
            non_numeric.append(s)
    return sorted(numeric) + sorted(non_numeric)

# ────────────────────────────
# Mapping helper
# ────────────────────────────
@st.cache_resource
def create_map(filtered_vehicle_df: pd.DataFrame):
    if (
        "vehicle_position_latitude" in filtered_vehicle_df.columns
        and "vehicle_position_longitude" in filtered_vehicle_df.columns
    ):
        filtered_vehicle_df = filtered_vehicle_df.dropna(
            subset=["vehicle_position_latitude", "vehicle_position_longitude"]
        )

    if filtered_vehicle_df.empty:
        map_center = [48.8566, 2.3522]  # Paris default
        zoom = 12
    else:
        map_center = [
            filtered_vehicle_df["vehicle_position_latitude"].mean(),
            filtered_vehicle_df["vehicle_position_longitude"].mean(),
        ]
        zoom = 12

    fmap = folium.Map(location=map_center, zoom_start=zoom)
    cluster = MarkerCluster().add_to(fmap)
    positions = []

    for _, row in filtered_vehicle_df.iterrows():
        lat, lon = row["vehicle_position_latitude"], row["vehicle_position_longitude"]
        positions.append([lat, lon])
        ts = int(row["vehicle_timestamp"])
        ts_fmt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        popup_html = "<br>".join(
            f"<b>{col.replace('vehicle_', '')}</b>: "
            f"{ts_fmt if col == 'vehicle_timestamp' else row[col]}"
            for col in filtered_vehicle_df.columns
            if col != "original_json"
        ).replace("vehicle_vehicle_id", "vehicle_id")
        folium.Marker(
            location=[lat, lon], popup=folium.Popup(popup_html, max_width=300)
        ).add_to(cluster)

    if positions:
        fmap.fit_bounds(positions)
    return fmap

# ────────────────────────────
# Data-filter helper
# ────────────────────────────
@st.cache_data
def get_filtered_data(vehicle_df, trip_df, filter_option, selected_value):
    v_df = vehicle_df.copy()
    t_df = trip_df.copy()

    if not selected_value:
        return v_df, t_df

    if filter_option == "Vehicle ID":
        if "vehicle_vehicle_id" in v_df.columns:
            v_df = v_df[v_df["vehicle_vehicle_id"] == selected_value]
        if "tripUpdate_vehicle_id" in t_df.columns:
            t_df = t_df[t_df["tripUpdate_vehicle_id"] == selected_value]

    elif filter_option == "Trip ID":
        if "tripUpdate_trip_tripId" in t_df.columns:
            t_df = t_df[t_df["tripUpdate_trip_tripId"] == selected_value]
        # Grab associated vehicles
        vehicle_ids = (
            t_df["tripUpdate_vehicle_id"].dropna().unique().tolist()
            if "tripUpdate_vehicle_id" in t_df.columns
            else []
        )
        if vehicle_ids and "vehicle_vehicle_id" in v_df.columns:
            v_df = v_df[v_df["vehicle_vehicle_id"].isin(vehicle_ids)]
        elif "vehicle_trip_tripId" in v_df.columns:
            v_df = v_df[v_df["vehicle_trip_tripId"] == selected_value]

    elif filter_option == "Route ID":
        if "vehicle_trip_routeId" in v_df.columns:
            v_df = v_df[v_df["vehicle_trip_routeId"] == selected_value]
        if "tripUpdate_trip_routeId" in t_df.columns:
            t_df = t_df[t_df["tripUpdate_trip_routeId"] == selected_value]

    return v_df, t_df

# ────────────────────────────
# Title
# ────────────────────────────
title_container = st.empty()
if "title" not in st.session_state:
    st.session_state["title"] = "GTFS Inspector"
title_container.title(st.session_state.title)

# ────────────────────────────
# Sidebar: Manage sources
# ────────────────────────────
st.sidebar.header("Manage Sources")

if st.sidebar.button(":material/refresh: Refresh", key="refresh_button"):
    clear_network_session_state()

action = st.sidebar.selectbox("Action", ["Add", "Modify", "Delete"], key="action")
network_list = load_network_list()

# --- Add
if action == "Add":
    with st.sidebar.form("Add Source"):
        name = st.text_input("Network Name")
        vehicle_positions_url = st.text_input("Vehicle Positions URL")
        trip_updates_url = st.text_input("Trip Updates URL")
        if st.form_submit_button(":material/add: Add to GTFS-RT list") and name:
            upload_or_update_network_file(
                name,
                json.dumps(
                    {
                        "vehicle_positions_url": vehicle_positions_url,
                        "trip_updates_url": trip_updates_url,
                    },
                    indent=4,
                ),
            )
            clear_network_session_state()

# --- Modify
if action == "Modify":
    name_to_modify = st.sidebar.selectbox("Select Source to Modify", network_list)
    if name_to_modify:
        existing = json.loads(download_network_file(name_to_modify))
        with st.sidebar.form("Modify Network"):
            vehicle_positions_url = st.text_input(
                "Vehicle Positions URL", existing.get("vehicle_positions_url", "")
            )
            trip_updates_url = st.text_input(
                "Trip Updates URL", existing.get("trip_updates_url", "")
            )
            if st.form_submit_button(":material/save: Save changes"):
                upload_or_update_network_file(
                    name_to_modify,
                    json.dumps(
                        {
                            "vehicle_positions_url": vehicle_positions_url,
                            "trip_updates_url": trip_updates_url,
                        },
                        indent=4,
                    ),
                )
                clear_network_session_state()

# --- Delete
if action == "Delete":
    name_to_delete = st.sidebar.selectbox("Select Source to Delete", network_list)
    if name_to_delete and st.sidebar.button(f":material/delete: Delete {name_to_delete}"):
        delete_network_file(name_to_delete)
        clear_network_session_state()

# ────────────────────────────
# Main-panel source selector
# ────────────────────────────
network_list = load_network_list()
if network_list:
    selected_name = st.selectbox("Select Source", network_list, key="select_network")
else:
    st.info("No networks available. Please add one.")
    st.stop()

# ────────────────────────────
# Load feeds
# ────────────────────────────
if st.button(
    f":material/system_update_alt: Load GTFS-RT {selected_name}",
    use_container_width=True,
):
    st.toast(":blue[:material/download:] Loading data…")
    try:
        definition = json.loads(download_network_file(selected_name) or "{}")
        if not definition:
            st.toast(
                f":orange[:material/error:] No data definition found for '{selected_name}'."
            )
            st.stop()

        vehicle_df, trip_df = pd.DataFrame(), pd.DataFrame()

        if definition.get("vehicle_positions_url"):
            feed = open_gtfs_realtime_from_url(definition["vehicle_positions_url"])
            if feed:
                vehicle_df = protobuf_to_dataframe(feed)
                st.toast(f":material/list: Vehicle data – {len(vehicle_df)} rows")

        if definition.get("trip_updates_url"):
            feed = open_gtfs_realtime_from_url(definition["trip_updates_url"])
            if feed:
                trip_df = protobuf_to_dataframe(feed)
                st.toast(f":material/list: Trip updates – {len(trip_df)} rows")

        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.session_state.update(
            {
                "vehicle_data": vehicle_df,
                "trip_data": trip_df,
                "selected_name": selected_name,
                "fetch_time": fetch_time,
                "title": f"{selected_name} – {fetch_time}",
            }
        )
        title_container.title(st.session_state.title)
        st.toast(":green[:material/check_circle:] Data loaded successfully!")
    except Exception as e:
        st.toast(f":orange[:material/error:] Error loading data: {e}")

# ────────────────────────────
# Display data (if any)
# ────────────────────────────
if all(k in st.session_state for k in ("vehicle_data", "trip_data", "fetch_time")):
    vehicle_data = st.session_state["vehicle_data"]
    trip_data = st.session_state["trip_data"]
    fetch_time = st.session_state["fetch_time"]
    source_name = st.session_state["selected_name"]

    if vehicle_data.empty and trip_data.empty:
        st.warning("No GTFS-RT data available.")
        st.stop()

    # 1️⃣ User filter
    filter_option = st.selectbox(
        "Filter by", ["Vehicle ID", "Trip ID", "Route ID"], key="filter_option"
    )
    selected_value = None

    if filter_option == "Vehicle ID":
        ids = smart_sort(list(vehicle_data.get("vehicle_vehicle_id", [])))
        selected_value = st.selectbox("Select Vehicle", [""] + ids)
    elif filter_option == "Trip ID":
        ids = set(vehicle_data.get("vehicle_trip_tripId", [])) | set(
            trip_data.get("tripUpdate_trip_tripId", [])
        )
        selected_value = st.selectbox("Select Trip", [""] + smart_sort(ids))
    elif filter_option == "Route ID":
        ids = set(vehicle_data.get("vehicle_trip_routeId", [])) | set(
            trip_data.get("tripUpdate_trip_routeId", [])
        )
        selected_value = st.selectbox("Select Route", [""] + smart_sort(ids))

    if selected_value == "":
        selected_value = None

    # 2️⃣ Filter data
    filtered_vehicle_df, filtered_trip_df = get_filtered_data(
        vehicle_data, trip_data, filter_option, selected_value
    )

    # 3️⃣ Layout
    col1, col2 = st.columns(2)

    # Map (right)
    with col2:
        st.write("Map View")
        if not filtered_vehicle_df.empty and {
            "vehicle_position_latitude",
            "vehicle_position_longitude",
        }.issubset(filtered_vehicle_df.columns):
            folium_static(
                create_map(filtered_vehicle_df), width=MAP_SIZE, height=MAP_SIZE
            )
        else:
            st.write("No vehicle positions available to plot.")

    # Tabs & downloads (left)
    with col1:
        if filtered_vehicle_df.empty and filtered_trip_df.empty:
            st.info("No rows match the selected filters.")
            st.stop()

        tab_labels, tab_frames = [], []
        if not filtered_vehicle_df.empty:
            tab_labels.append(f"Vehicle Positions ({len(filtered_vehicle_df)})")
            tab_frames.append(filtered_vehicle_df)
        if not filtered_trip_df.empty:
            tab_labels.append(f"Trip Updates ({len(filtered_trip_df)})")
            tab_frames.append(filtered_trip_df)

        tabs = st.tabs(tab_labels)

        for idx, tab in enumerate(tabs):
            df = tab_frames[idx]
            with tab:
                # Choose a stable data-type label:
                label = tab_labels[idx]
                data_type = (
                    "Vehicle_Positions"
                    if label.startswith("Vehicle Positions")
                    else "Trip_Updates"
                )

                # JSON list from original_json column
                json_list = [json.loads(j) for j in df["original_json"]]
                json_str = json.dumps(json_list, indent=4)

                filename_base = (
                    f"{source_name}_{data_type}"
                    f"{'_Filtered' if selected_value else ''}_{fetch_time}"
                    .replace(" ", "_")
                    .replace(":", "-")
                )

                st.download_button(
                    ":material/download::material/data_object: Download JSON",
                    data=json_str,
                    file_name=f"{filename_base}.json",
                    mime="application/json",
                    key=f"{data_type}_json_{idx}",
                )

                towrite = io.BytesIO()
                df.to_excel(towrite, index=False)
                towrite.seek(0)
                st.download_button(
                    ":material/download::material/table: Download XLSX",
                    data=towrite,
                    file_name=f"{filename_base}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"{data_type}_xlsx_{idx}",
                )

                st.json(json_list)
