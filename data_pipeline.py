# data_pipeline.py
"""_summary_: Automate incremental data ingestion pipeline

Author:
    _description_: The script is developed and maintained by Raju Chowdhury
    _developer_: raju.chowdhury@molgroup.com
    _history_:
        - 19-Nov-2025: Initial version created
"""

import logging  
import os
from dotenv import load_dotenv, find_dotenv
from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd
from datetime import datetime
import warnings
import io

# 1. Setup Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Log setup successfully!!!")

# 2. Column Schema
# Note: 'Job No.' column name will be dynamically set by the KEY_COL environment variable
udf_cols = {
 'Job No.': 'key_col_placeholder', 
 'carr_eqp_uid': 'carr_eqp_uid',
 'Container Number': 'container_number',
 'Container Type': 'container_type',
 'Destination Service': 'destination_service',
 'Consignee Code (Multiple)': 'consignee_code_multiple',
 'PO Number (Multiple)': 'po_number_multiple',
 'Booking Number (Multiple)': 'booking_number_multiple',
 'FCR Number (Multiple)': 'fcr_number_multiple',
 'Ocean BL No (Multiple)': 'ocean_bl_no_multiple',
 'Load Port': 'load_port',
 'Final Load Port': 'final_load_port',
 'Discharge Port': 'discharge_port',
 'Last CY Location': 'last_cy_location',
 'Place of Receipt': 'place_of_receipt',
 'Place of Delivery': 'place_of_delivery',
 'Final Destination': 'final_destination',
 'First Vessel Code': 'first_vessel_code',
 'First Vessel Name': 'first_vessel_name',
 'First Voyage code': 'first_voyage_code',
 'Final Carrier Code': 'final_carrier_code',
 'Final Carrier SCAC Code': 'final_carrier_scac_code',
 'Final Carrier Name': 'final_carrier_name',
 'Final Vessel Code': 'final_vessel_code',
 'Final Vessel Name': 'final_vessel_name',
 'Final Voyage code': 'final_voyage_code',
 'True Carrier Code': 'true_carrier_code',
 'True Carrier SCAC Code': 'true_carrier_scac_code',
 'True Carrier SCAC Name': 'true_carrier_scac_name',
 'ETD LP': 'etd_lp',
 'ETD FLP': 'etd_flp',
 'ETA DP': 'eta_dp',
 'ETA FD': 'eta_fd',
 'Revised ETA': 'revised_eta',
 'Predictive ETA': 'predictive_eta',
 'ATD LP': 'atd_lp',
 'ATA FLP': 'ata_flp',
 'ATD FLP': 'atd_flp',
 'ATA DP': 'ata_dp',
 'Derived ATA DP': 'derived_ata_dp',
 'Revised ETA FD': 'revised_eta_fd',
 'Predictive ETA FD': 'predictive_eta_fd',
 'Cargo Received Date (Multiple)': 'cargo_received_date_multiple',
 'Detention Free Days': 'detention_free_days',
 'Demurrage Free Days': 'demurrage_free_days',
 'Hot Container Flag': 'hot_container_flag',
 'Supplier/Vendor Name': 'supplier_vendor_name',
 'Manufacturer Name': 'manufacturer_name',
 'Ship To Party Name': 'ship_to_party_name',
 'Booking Approval Status': 'booking_approval_status',
 'Service Contract Number': 'service_contract_number',
 'CARRIER VEHICLE LOAD Date': 'carrier_vehicle_load_date',
 'Carrier Vehicle Load Lcn': 'carrier_vehicle_load_lcn',
 'Vehicle Departure Date': 'vehicle_departure_date',
 'Vehicle Departure Lcn': 'vehicle_departure_lcn',
 'Vehicle Arrival Date': 'vehicle_arrival_date',
 'Vehicle Arrival Lcn': 'vehicle_arrival_lcn',
 'Carrier Vehicle Unload Date': 'carrier_vehicle_unload_date',
 'Carrier Vehicle Unload Lcn': 'carrier_vehicle_unload_lcn',
 'Out Gate Date From DP': 'out_gate_date_from_dp',
 'Out Gate Location': 'out_gate_location',
 'Equipment Arrived at Last CY': 'equipment_arrived_at_last_cy',
 'Equipment Arrival at Last Lcn': 'equipment_arrival_at_last_lcn',
 'Out gate at Last CY': 'out_gate_at_last_cy',
 'Out gate at Last CY Lcn': 'out_gate_at_last_cy_lcn',
 'Delivery Date To Consignee': 'delivery_date_to_consignee',
 'Delivery Date To Consignee Lcn': 'delivery_date_to_consignee_lcn',
 'Empty Container Return Date': 'empty_container_return_date',
 'Empty Container Return Lcn': 'empty_container_return_lcn',
 'Late Booking Status': 'late_booking_status',
 'Current Departure status': 'current_departure_status',
 'Current Arrival status': 'current_arrival_status',
 'Late Arrival status': 'late_arrival_status',
 'Late Container Return status': 'late_container_return_status',
 'CO2 Emission For Tank On Wheel': 'co2_emission_for_tank_on_wheel',
 'CO2 Emission For Well To Wheel': 'co2_emission_for_well_to_wheel',
 'Job Type': 'job_type',
 'MCS HBL': 'mcs_hbl',
 'Transport Mode': 'transport_mode'
}


# 3. Utility Functions (from original script)

def list_csv_blobs(key, con_name):
    """Lists all CSV blob names in a container."""
    bsc = BlobServiceClient.from_connection_string(key)
    con_client = bsc.get_container_client(con_name)
    csv_files = [
        b.name for b in con_client.list_blobs()
        if b.name.endswith('csv')
    ]
    return csv_files

def find_latest_csv(file_list):
    """Finds the most recent CSV file based on the datetime in the name."""
    res = sorted(
        file_list,
        key = lambda item: datetime.strptime(item.split('_')[1].split(".")[0], '%Y%m%d%H%M'),
        reverse=True
    )
    return res[0]

def read_file(key, con_name, file):
    """Reads a CSV file from Azure Blob into a Pandas DataFrame."""
    bsc = BlobServiceClient.from_connection_string(key)
    con_client = bsc.get_container_client(container=con_name)
    blob = con_client.get_blob_client(file)
    b_file = blob.download_blob().content_as_text(encoding='iso-8859-1')
    df = pd.read_csv(StringIO(b_file),low_memory=False, parse_dates=False)
    return df

def normalize_string(val):
    """Normalizes string values."""
    if str(val).strip() == "()":
        return ""
    return str(val).strip().upper()

def preprocessed_df(df):
    """Applies string normalization to all object columns."""
    pre_df = df.copy()
    for col in pre_df.select_dtypes(include='object').columns:
        pre_df[col] = pre_df[col].apply(normalize_string)
    return pre_df

def upload_df_to_blob(connection_string, container_name, blob_name, dataframe):
    """Uploads a DataFrame as a CSV file to Azure Blob Storage."""
    output = io.StringIO()
    dataframe.to_csv(output, index=False, encoding='utf-8')
    data_to_upload = output.getvalue()
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    blob_client.upload_blob(
        data=data_to_upload, 
        overwrite=True, 
        blob_type="BlockBlob"
    )
    logger.info(f"Successfully uploaded and overwrite new data into blob: '{blob_name}'")


# 4. Main Execution Function for FastAPI
def run_ingestion_pipeline():
    """
    Main execution logic for the incremental data ingestion pipeline.
    This function will be called by the FastAPI endpoint.
    """
    warnings.filterwarnings('ignore')
    pd.set_option('display.max_columns', None)
    
    logger.info("Starting data ingestion pipeline.")
    
    # Load ENV variables
    load_dotenv(find_dotenv(), override=True)
    conn_str = os.getenv("AZURE_BLOB_CONNECTION_STRING")
    dnld_con_name = os.getenv("AZURE_BLOB_CONTAINER_NAME_DNLD")
    upld_con_name = os.getenv("AZURE_BLOB_CONTAINER_NAME_UPLD")
    key_col_original = os.getenv("KEY_COL") # e.g., 'Job_No'
    pre_pblob = os.getenv('PRE_FILE_NAME') # The name of the file in the upload container

    if not all([conn_str, dnld_con_name, upld_con_name, key_col_original, pre_pblob]):
        raise ValueError("One or more required environment variables (AZURE_BLOB_*, KEY_COL, PRE_FILE_NAME) are missing.")

    # Update the key column in the schema for renaming
    udf_cols['Job No.'] = key_col_original
    
    # 1. Find and read the latest incremental file
    csv_files = list_csv_blobs(key=conn_str, con_name=dnld_con_name)
    latest_csv = find_latest_csv(csv_files)
    logger.info(f"Most recent csv file found: {latest_csv}")

    w_df = read_file(conn_str, dnld_con_name, latest_csv)
    initial_shape = w_df.shape
    
    # 2. Preprocessing
    
    # Normalize column names (remove newlines)
    for col in w_df.columns.to_list():
        if "\n" in col:
            ncol = col.replace("\n", " ")
            w_df.rename(columns={col: ncol}, inplace=True)
    
    # Rename columns as per schema
    r_df = w_df.rename(columns=udf_cols)
    
    # Normalize string values
    curr_df = preprocessed_df(r_df)
    
    # Type conversion (Boolean mapping)
    bool_cols = ['hot_container_flag', 'late_booking_status', 'current_departure_status','current_arrival_status', 'late_arrival_status', 'late_container_return_status']
    for col in bool_cols:
        if col in curr_df.columns:
            curr_df[col].replace({'Y': True, 'YES':True, 'N': False, 'NO': False}, inplace=True)

    # 3. Incremental Merging Logic
    
    # Read existing data from production
    logger.info("Reading existing data from production container...")
    prev_df = read_file(conn_str, upld_con_name, pre_pblob)
    
    # Remove records from previous file that are being updated by current file
    unique_jobs_curr = curr_df[key_col_original].unique()
    mask_keep = ~prev_df[key_col_original].isin(unique_jobs_curr)
    prev_df_cleaned = prev_df[mask_keep].copy()
    
    # Merge (concatenate) the old clean data and the new data
    final_df = pd.concat([prev_df_cleaned, curr_df], ignore_index=True)
    
    # Final check on duplicates (drop_duplicates with keep='last' for safety)
    # final_df = final_df.drop_duplicates(subset=[key_col_original], keep='last')
    
    # 4. Upload Final Data
    logger.info(f"Uploading final merged data (Shape: {final_df.shape}) to production blob: {pre_pblob}")
    upload_df_to_blob(
        connection_string=conn_str, 
        container_name=upld_con_name, 
        blob_name=pre_pblob, 
        dataframe=final_df
    )
    
    logger.info("Pipeline completed successfully.")
    
    # Return summary data for the API response
    return {
        "status": "SUCCESS",
        "total_records_in_final_file": len(final_df),
        "new_records_shape": initial_shape,
        "previous_records_shape": prev_df.shape,
        "key_column_used": key_col_original,
        "target_blob": pre_pblob
    }
