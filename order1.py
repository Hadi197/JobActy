import requests
import pandas as pd
import time
import brotli
import json
import datetime
import os
import pytz

# API URL
url = "https://phinnisi.pelindo.co.id:9018/api/executing/spk-pilot"

# Parameters for GET request
today = datetime.date.today()
start_date = today
end_date = today
params = {
    "page": 1,
    "record": 10000,
    "data": "",
    "start": start_date.isoformat(),
    "end": end_date.isoformat()
}

# Headers from the request
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,id;q=0.8,ms;q=0.7",
    "access-token": "eyJhbGciOiJIUzI1NiJ9.VTJGc2RHVmtYMTl6RGtlQ1JmZENyejBDZmprcW5wcUloUUp0SHN6RnhJRUZNOWhXblE4SERQQ1Z0VjM2ajhzYXl0M29CSDZ1UFNZaU9VTEdDNUJoZXMzRHV4cHQ1OU9iaWJYMklqNitJRU1JSTR4N3d5VWN1Y2RmQ29reEFlWnlBMGllUmVjZmV2ZFM5ZEhCK1VucnZiSE5DS2hyZTY1TTREU3BqZmQ3WTYrbkkyVitCOFhUcmVIM0xkUjh5WEdiOVUrU09ORXIyZy8wakFVQk9MeiszK1p2NWliRTZKdUpWc1lWVjdhYmJnMUpLMnJ6L1RaYlY2dVByUDVMa29HTzlFcmhoZWV6eUpTNWEvdDVNcDgrOGZsL0NuaXhDSU0wbzJnK3ZCNm5WdmxVeDVHUmZtc3ZhVldhTnpNbHV1SzhXK0EyZnJpeHIrbktNcTdqaHI1WTNaZ2RnVWxObDdOWVJYbVZiM2FMVTdXUjlrKzY4TGZPNE5BMHc4bXVJNk5TSTFvLzNIOGJHSEJNNGk0MjhpM1dER2svTkZBTDJaS1dPc04zcjR6WnREUC8zS2FvNVBHZ1FPdmJXbTNUU2FCaHBIVEpXbUNTSlRzTTdiT2krcCtiSEIvWER5WkVwWEZxbitrS0tKdm5DTDVYNWowc1hPaHVQS0xDSm9Xbmk3Ri80eXJtZW5KSnNFcjBiZWZEakd1ZVVZdlFGc3FGM21rMldYR0Q2aG1ndmNrWnJqMFFCelNNaHlvcWVyT0RnVzJlS1gxdHFOaU90WTYzTENBN1ZqVkhSY3VMaWtNcmpoR2p4em9oUGJWNXJYWWk2VktYazg2MlZOWG16NWREend5QjBrdTZBdmdpVGZZaWdPTDgzb3pjNDVrMDZRb3ZJS3d0RlBkNmVFb05BaEg0dng3b0huZEhWWTd6NEpOemVLR2Z6bHVyUUQvVUREZk0xdUFIbE1pVGxMRlVIRzVlS0ZTKzJnRklnTGFLS3hhZkRKVmsrV0RwUi8xVlN0QkJwbzh0aW9QM1JDcG5JWU1DSXRpeFRnK0h5QndDV25kUHEvdzFqWEwvei9YMGlET0E2b2RwMHRjOXdnaTA5bzVpNzBwcTFneE42UGNVTVAxOUxpLzR4VWh2d3AxWnE5OHZNOWovc1B5Q2VZZEk0dUg3c0FNTC9sVkJVeld1YkZsL2tHZ01seXlnbS9CbWtERFM0bnA0dXhOdndQQUFhSldXaTJkS0tSa1RhMnBNQUYvTDd0c1NJSk1YWHJsTXdHM0xKT21QMTNpS1VlazM1cFZYZFZFSW54ZUpWRTlFWjNKL1FOY1VkVEFLZ0pwWWozSDhRaE1TR24ybEkyeGVKa3BGR3lSbGNMbUZyR3Nwd2Z0SXlwT1FVdklxWkhwNW1SbWE3L3hDR3IyS3dWWkJlUGI5bWFxdktoK2psbTN4UXNCSE9OZ2lEWUJoTGpXblc0b3JEREsvTmF5VUxrOHdpaXp2T0VveFVxelZGaG1MRlJFTWF1NmcrOWRZR0FpTmIwUXlqNDhFdWxnNlVCY09MV3BhMUJlZWs3eHNZR3MrOVd4UW5pY1JuNFBvQzlLOUd4NmNUUzVpeFdJcmNaZk1HdzlrOVVHa3RuenlkT0JreHM5aWRUNWgyWTVsUkRzWitYNUI5M2lVSWc0ck1ENUlQSmF5M1ZqMVl1WjV0OFZ0OVlKK3Z0bjV1aldodFM1VDR1b09POElieEorUlVSZkVubExWeE5qdGE2VnhxaGd6SWxtcEdPbFZuS2xlMDluT0JmWlNvUm1zeWFJTUh5dGI4Sy8xc0ZEWmhzNW5tYWlxbFdCbElQK3o5Znk0WGIxd0tEVzVTYmR0OHYrN0Rsamp6c203ejZkVmdCNllIUFVjbXJoR1cwckxyOWFTUkI4UXYyQzNEdTBONEdsa2gwK1lGVWVpaFhBbmplRThxVHFpTUhVQnZ0WDd2bTJJU1Y4a0dIdEF3THgrOEg2b3FUdEg5YVFiVm9IbWVaUmhYLy96WW0wbnNZd1JHMTB6L1ZLUHcrWWR2MForWVhTOGhmRTVNT3FzUk80Qzc0UDIyRDB3WXB5YlBIRlh4TzhMUWV4NkFWSStydjFyVkgyMkdhbjZDVC9zblhDMHBIMDVNaFN3ckIweHZzVU1XSUV3Ujh1MzZJS2RGWFMxdXJrQUR3dnNFNEF6MVRmdGVSU2ZMMm5UV0I1dE9raE9OYkZ5akRPOUxPbFhSNENjT3BFOXQ0d1lkNFlSMEUzS2I1OGFTMTRpSk1iak85QXl5ejYzR2gwSTVySXRBT3hoVTh2YkxjbmFLdmt1Nkl3dzBvZUxFMG0yRUtFWWF6Qm0xUE1jNzdnaTlXRGRURWVEWnJFcVU2WEZaSFBoODN4YzlHbHJDMEl5ZmpiMGx5WXZkbUdJcENkcXFPQ1BUQmVMM0Z5ZEludVZrdUQvNTVvZmNRM3Q3N2hJTVFmU2cwS2k0L2MwSkIyTjJaYTl6ditZWS8vZlBGa2NrN2ZLTllkMWdxZ0NJYmVnVzJtcm5RSFdmMjR0UG5rb1JGdVk0c0NVMExFME5vL3hOWlBHNUtBYkxCdzhtZ2RLQW0xK2Y0T05LbHJiSHdadldVcFZCWFBPZUdWV2tEVTBTV21lT2lMSXZyY0NkTE1JaUVxK3VFaDBOVHU4VlJEWGFYbkUxR1k0RmNCQVMvdDlQaWM4REh4ZzVqZ0lSSTl0WW8zS0ppM212MVB0NWJjRC9vN0dBcDdMVUNxN0NENFZYNGRXUHByVkhvZ0RpVzRKZDdqUGpWbzBWNm5QY3BvaGs0NXBsUnJuZjk4cVlkb0NQSjg3YXNVdFpGSkt3enN1ZWREbzJDUlRoZ3FCVVQ2eEZKY3NDVFAva29wM0FoYjZORll4TjM4eFBjVDdFSGJuMWR2OHBTbmlyd1gvcWd6MnRLNHFUYVR4MmZ6Y3NWYXV0c2M1VHZYVFhEQWl3dTc4cHU3Rld0NS8xT0J3WE5BU2hOZEd2azdDZy9BRnJlNVpXcXJ5bzYwbWRUNCs0TG5VRFZJTzdkY0tCL2o5L0VCczhwRmRIeFJnVm1CV2dZRHJRUks3Qld0UVgweHhtYU4zRi9vUGRFQzlOWEh3YTNHb1h0WExVMjZVN3BNLzZ1RWE1ZXVJOWpIZXB5T3dia2NqTUFQMGJtUDBoODBzSkZKMElNdlYyYk91ZkVuVnlRdHc0ckRmaFg3SG1NS21NVk84ZStVN1JZL2Q.rLrUqeEKgwTU9X0Aq5OeiG2otuzmx4SMt9YwzfZGzBc",
    "connection": "keep-alive",
    "host": "phinnisi.pelindo.co.id:9018",
    "id-unit": "",
    "id-zone": "",
    "origin": "https://phinnisi.pelindo.co.id",
    "referer": "https://phinnisi.pelindo.co.id/",
    "sec-ch-ua": '"Opera";v="120", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "sub-branch": "MTQy,ODI=,NDc=,NTE=,NTI=,ODg=,ODc=,ODY=,Mzc=,NDA=,NTA=,NDg=,ODQ=,Mzg=,MzE=,NjM=,Mzk=,MTA0,MjA=,Mg==,MTEy,NA==,NQ==,Ng==,OQ==,MTA=,MTE=,MTI=,MTM=,MTQ=,MQ==,MTEx,MTEw,MTA2,MTA1,Mw==,NzU=,NzI=,NTg=,NjY=,MTAx,NjE=,ODE=,MjU=,NTQ=,ODM=,NzQ=,NzM=,NjI=,NTY=,NDk=,NDQ=,Njk=,Njg=,Mjk=,Mjc=,NzE=,MTc=,NTc=,NjU=,MTY=,MTA4,NjA=,NjQ=,MTk=,MTU=,MTg=,MjY=,MjQ=,MzM=,MzQ=,MzY=,NTM=,MTAz,NDU=,OTg=,OTA=,NzY=,Nzc=,OTM=,MjI=,MjM=,Mjg=,MzA=,MzU=,MzI=,NDE=,NDM=,ODA=,ODU=,NTU=,NTk=,MTAy,OTI=",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0"
}

# Pilih hanya kolom yang diinginkan
desired_columns = [
    'data.id_order_header',
    'data.order_no',
    'data.type_name',
    'data.ppkb_description.id_ppkb_ppkb.id_pkk_pkk.no_pkk_inaportnet',
    'data.id_pilot_type',
    'data.pilot_request_time',
    'data.approval_date',
    'data.integration_data.integration.no_spk_pandu',
    'data.integration_data.integration.spk_success_message.pilot_name',
    'data.integration_data.integration.spog_success_message.valid',
    'data.integration_data.integration.spk_success_message.pilot_date',
    'data.integration_data.integration.spk_success_message.location_name',
    'data.external_vessel.vessel_name',
    'data.external_vessel.grt',
    'data.external_vessel.loa',
    'data.trx_order_assigned.id_pilot_mst_pilot_profile.full_name',
    'data.branch.name_branch',
    'data.ppkb_guide.estimated_start_time',
    'data.ppkb_guide.estimated_end_time',
    'data.ppkb_guide.guide_request_hours_change',
    'data.request_time',
    'data.timezone_name',
    'data.trx_order_pilots.pilot_deploy',
    'data.trx_order_pilots.pilot_arrive',
    'data.trx_order_pilots.pilot_onboard',
    'data.trx_order_pilots.pilot_start',
    'data.trx_order_pilots.pilot_end',
    'data.trx_order_pilots.pilot_off'
]

simple_names = [
    'id_order_header',
    'order_no',
    'type_name',
    'no_pkk_inaportnet',
    'id_pilot_type',
    'pilot_request_time',
    'approval_date',
    'no_spk_pandu',
    'pilot_name',
    'valid',
    'pilot_date',
    'location_name',
    'vessel_name',
    'grt',
    'loa',
    'full_name',
    'name_branch',
    'estimated_start_time',
    'estimated_end_time',
    'guide_request_hours_change',
    'request_time',
    'timezone_name',
    'pilot_deploy',
    'pilot_arrive',
    'pilot_onboard',
    'pilot_start',
    'pilot_end',
    'pilot_off'
]

# Create a session for connection reuse
session = requests.Session()
session.headers.update(headers)

# Send GET request with retry and pagination
max_retries = 3
all_data = []
page = 1
while True:
    params["page"] = page
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=30)
            break
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise
    # Check if request was successful
    if response.status_code == 200:
        try:
            # Try to decompress if brotli compressed
            try:
                decompressed = brotli.decompress(response.content)
                response_text = decompressed.decode('utf-8')
                response_data = json.loads(response_text)
            except brotli.error:
                # Not compressed, use response.json()
                response_data = response.json()
        except Exception as e:
            print(f"Failed to decode response: {e}")
            print(f"Response status: {response.status_code}")
            print(f"Response text: {response.text[:2000]}")
            exit(1)
        # Extract the 'data' array from the response
        if 'data' in response_data and 'dataRec' in response_data['data'] and isinstance(response_data['data']['dataRec'], list):
            data_page = response_data['data']['dataRec']
            if data_page:
                all_data.extend(data_page)
                page += 1
                print(f"Fetched page {page-1}, total records so far: {len(all_data)}")
                time.sleep(1)  # Delay between requests
            else:
                break  # No more data
        else:
            print("No data found in response")
            break
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(f"Response text: {response.text[:1000]}")
        break

# Process all collected data
if all_data:
    # Extract only the specified columns
    extracted_data = []
    for item in all_data:
        extracted_item = {
            'id_order_header': item.get('id_order_header'),
            'ppkb_code': item.get('ppkb_code'),
            'company_name': item.get('company_name')
        }
        extracted_data.append(extracted_item)
    
    df = pd.DataFrame(extracted_data)
    df.to_csv('job1.csv', index=False)
    print(f"Data saved to job1.csv with {len(extracted_data)} records")
    
# Now fetch details for each id_order_header
if os.path.exists('job1.csv'):
    job1_df = pd.read_csv('job1.csv')
    id_list = job1_df['id_order_header'].dropna().astype(int).tolist()
    all_detail_data = []
    for order_id in id_list:
        url = f"https://phinnisi.pelindo.co.id:9018/api/jobactivities/detail-order/{order_id}"
        print(f"Fetching details for order ID: {order_id}")
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and 'data' in data and data['data']:
                    df = pd.json_normalize(data)
                    for col in desired_columns:
                        if col not in df.columns:
                            df[col] = None
                    df = df[desired_columns]
                    rename_dict = dict(zip(desired_columns, simple_names))
                    df.rename(columns=rename_dict, inplace=True)
                    df['id_order_header'] = order_id
                    
                    # Adjust times based on timezone_name
                    time_columns = ['pilot_request_time', 'approval_date', 'pilot_date', 'estimated_start_time', 'estimated_end_time', 'guide_request_hours_change', 'request_time', 'pilot_deploy', 'pilot_arrive', 'pilot_onboard', 'pilot_start', 'pilot_end', 'pilot_off']
                    
                    # Map timezone abbreviations to pytz timezone names
                    timezone_map = {
                        'WIB': 'Asia/Jakarta',
                        'WITA': 'Asia/Makassar',
                        'WIT': 'Asia/Jayapura'
                    }
                    
                    if 'timezone_name' in df.columns and not df['timezone_name'].isna().all():
                        tz_abbr = df['timezone_name'].iloc[0]
                        tz_name = timezone_map.get(tz_abbr, tz_abbr)  # fallback to original if not mapped
                        try:
                            tz = pytz.timezone(tz_name)
                            for col in time_columns:
                                if col in df.columns and not df[col].isna().all():
                                    # Assume times are naive and in the specified timezone
                                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(tz).dt.tz_convert('UTC')
                        except Exception as e:
                            print(f"Timezone conversion failed for {tz_name}: {e}")
                    
                    all_detail_data.append(df)
                    print(f"Details fetched for order ID: {order_id}")
                else:
                    print(f"No data for order ID: {order_id}")
            else:
                print(f"Failed to fetch details for order ID: {order_id} - Status: {response.status_code}")
        except Exception as e:
            print(f"Error fetching details for order ID: {order_id}: {e}")
        time.sleep(0.3)  # Rate limit
    if all_detail_data:
        combined_df = pd.concat(all_detail_data, ignore_index=True)
        # Join with job1.csv
        job1_df = pd.read_csv('job1.csv')
        # Ensure id_order_header is int in both dataframes
        combined_df['id_order_header'] = combined_df['id_order_header'].astype(int)
        job1_df['id_order_header'] = job1_df['id_order_header'].astype(int)
        merged_df = combined_df.merge(job1_df, on='id_order_header', how='left')
        merged_df.to_csv('job.csv', index=False)
        print(f"Detail data joined with job1.csv and saved to job.csv with {len(merged_df)} records")
    else:
        print("No detail data to save")
else:
    print("job1.csv not found")