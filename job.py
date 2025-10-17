import requests
import json
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# Header dengan access-token
headers = {
    "accept": "application/json, text/plain, */*",
    "access-token": "eyJhbGciOiJIUzI1NiJ9.VTJGc2RHVmtYMStVTG5GVWJtcWVRWGFFWStlNVRoNEQ3UGFJbmRrTDMyMndnd1Z0ZEkzK0p6Q2NrNEZQQTg4d2phUkJsUlRla2tkU0JqUGJodGlsMDUybVlWVzJJVXVxTHJlcVJ1aU5jV1VKOE9tK1kzYUNqRmVzVXUrb0xUSzczNkc0TXhmbTdMakQ1WVpnMFlkRGdHZWJNTGtyRFlyUkwvY1NFWkNvakVIV2lvbEQ1U0RHUTcvZWdaMG5LZVdJRGxDOWsrQ0dGN3l5aWQrUXVJd1VCcGUyUDJuM28zVnR4ZU4vZlNnak1zcHVySllrZGdJbjhSSkFRQjVUdU9OblFSbGJnSHRvY3JvUnRwVzBpS1JXOWw0bXBHS2NPZVdPNnJqN3kwNzhlSTNZQ0lySUZkY0labWdnSHpxcHZxWmtpQXBvUFN5Qi9yTzBwYk1wLzhFQnlVRC9XUHd4RlI0NDQ3UEE2UDNUT1QzSWZKdW9xY2RPTHJpNXJNc2VEU2JTbUdiK29aSmpxcjZsWXJldXVSdVliQk9EQ3hjdnFEOXRPMnlyelFKRGx3ZHY5djNLUTFFd2IzUFg2Q1lzQXExc1BvVHk0cGlObmZmUjV1UWRUZnV2M1ByS2k4QVFabFZhT0FBV1I2ZGp4MzFRS3ZDWTFxemhxOVhGNTFzZk84UnpicWV1V0k2K3liVy91Q20zNWxCWlR0YSt5Nk95ZHVWZWcvU1Bsck1pMVcrTnJQVjhZMHpyTXlvZlFCdFUvWnhJNGZFdnowMEdXaU1BWThiWTJaSzJWaExqUEh2Q1hKT21ENXViTzREcnMzeUwwQ01HcE83SitWNmpZeU9jZHFqUlhDRlBHMXhWZVJzRmhDZjV3TmFNQVRIZmJSTjJtc0FkZ0JaVWM4TW4xMHJOTnE5azFVOC83L3psSUZ6T1Y0UzZNTUsvVitBV0VhUXFJSS8xamdYdDBWaHA4Si83eHM4andMR3djQ2MweDEyWVNFRWVzVE92SEo0azZSUmlabXdFL2tXNWMwL1pEZ0hxb3lCYXB0WDBEVENidWh5SlQzTzVzWGYyc0JFWmxsRU9aMjhTV05GYUhqUkNLa3VpWDljYStRNXNpOUh0NWVtT2hnenorcEpXU015R24rVS8xclhqU2F2YW8wc3BYTWVTK29Zd0pTWlVLZVVReXIrY1pMNFo5YWVaZEZDSFhZNzA5alNVRnJSTmRuZ0J2NUticXlXRDFHaHpDT1dabllTbENiZzlUWHBJOE5jSy9CZ0phUkZxMll1ZUZJc3loRjBsME5iTGZKNnZuSng3OGJUa24vaEk4ckNzY1EvVkRYL3pYNVBpWWFGOFZMWVJCNmN2MXlwQWRITzdEbGlEbnFiRkdsTFV1WnYwUmNPd2dzV2U5UE8vZ25NT1dMZ0MyTTVjczZMdlRCK0RNMjgrcVFMUUlHSWtuZC96c0c5L2J6UjhNNjRSbi8zQWZldE9ieE5wQk5nNW5YZzFmNDRnVWhRZ3RSdHBwY3R4ZS84Mi85ZitUeHVvRDdOMVErY1d4NWpFT2hFNXo3Um5ESXgva1g2L0dteFo1UXVGaUlCczkvK2lxSHlobEM3NWNzUnh4SjBkcGRPTzhKcmdsT0pTUDBRYWZRU2ovMFh5b2dRejV2Yk9DelpJNUx3Wlk2QUFMUjRIeHVmYXErdE82eEd5RzdkdFBaZDBuek5jY3A4K25BeUh6ZkxMbHR2S0RzUHVEaUhOd011enBKeEVTZ1ZaVEdVYStsb0hUME5KMzd3cS9PNS9GeWtSdDBNMWIrcWJiVkRlbkxiVlZPcjMwa3ZJcWJYdUx2NC8xVy9ZQmtrRitXQUpybUFOOTh5QXE3R1dndGJuMjlQYll1bGIxd3o5NmhLNjVOK3JIdUxnNTZsL1NDeWR6MyttcUIxK3JldkY5N3daM3hkek5TM29pNlloOGp0Sk9BaEN3Vlk3aUZGQVJoek9nQVZudFhOZjNGdk1lOG9VZldJbWRTZnZTdXNLQ01FY09uQ2tpcDhMb3dpVVEyRXZXUGo4djZTc1ZURnBTS0tXV20vcWtaTUhXa3I3VUZNWVROQ1Z5TXhjVGxITHlZRXR5b1JaYVppNG9mVWllak9aaytKeTVjWC90alBkL0pncFlqZnAySTZHcHkvOGdkdTRhZjQ2aFpEd2crZjdiZXYva2J6RkcrVVR2WnFyQ2ZmWXh0VlpBWHd0aUoySDIyM0wvWUZ2UVR5VlFWdFRoenRsRFFsWndkNlRPUjZ3dkxzQmVzWmp6N0RlM1RNUTVnSnpFaHNEeHJpMkxmV3ZjdUx5RFYrUHl5c3ZQNWxvUVozZnU5aDRaTWRLZXhkbGozT2dEbytJYzErTVpXYmtmQ25NeUlUMUZTM2M5Z052Zk1PL1I1bDhadks5VTRKaUlsRWkxb056V0FNZlptRHZ3dmlJbm9nNEp5ZjZWUkdMb3VQRkE5K2tEZzRFNFZKcElIQUE3ekJRM1llMDE3TitOS2d5RXRqcjV6M2FCd0tINGxBZ0pvcnhsMFRXNm84ZGNKWlNoRVUwc1E2ZGdsWlNvazlhd055dDNtbE1pMFVJNVhDODViWWpOK3RiTkJXa0FiRGZGRVFESTVpTStPSDU3YVplZzUyOUZIMVd1MXV6dko0d1dpUS96TGpmNVdudEFCWWl3TURuN01zdjFVaDhzbzRFVC9yanBaNzFCTS9Ga0hqUkIrL0d4NGdnSVlPMTBLT1c1bWFMdE9QU1VPRC9oNTdpV25Ob2gzYXovamszY1l4c0lHNm5sb1V5eng0L1Z0dGR3MUxFODQzUE02U3JURHVRSitEOGg4SkN0eXFnQXFwMXRiQjR4bmN5WVkvKzJuWnBGSmdzSUp1QW1UNWVyd2FzTzlla1hHaFd1bkV2NmtNcGNHVmliblR6OFhRMHRiYXNHMXdIT1pOMDMzMHpPWHpvZVZac2tVSExzSHpVMEJ5ZE1ka3k3Sm50NjJ6MC9nZUYveTZJdVhoZ1loQWxzNjRXOWMyenBISkZHdStjL0Jzc2RQODV2eU5GL3FnSVRzTHMxTlFlV2FncmVPNXJPZjFwSTViazlxcFpSK2I1aXAySW9aYnNzMCtPdHozMVJTMGMvazZvTkRtd0EvdTBLYjZDL2hDY0JqQUp1cVB4Y3M2YVNkMU53UzA2U3plaXZkVVBoazFjQWZaUk43ZHRRK2Y0dGR0SFdhNkJqRmZvL3ZiRFlKb1A.gTMAZPqNLRYWlw39P3wq0eVsxzX5YYhYrZvARVNyPVE",
    "sub-branch": "MTQy,ODI=,NDc=,NTE=,NTI=,ODg=,ODc=,ODY=,Mzc=,NDA=,NTA=,NDg=,ODQ=,Mzg=,MzE=,NjM=,Mzk=,MTA0,MjA=,Mg==,MTEy,NA==,NQ==,Ng==,OQ==,MTA=,MTE=,MTI=,MTM=,MTQ=,MQ==,MTEx,MTEw,MTA2,MTA1,Mw==,NzU=,NzI=,NTg=,NjY=,MTAx,NjE=,ODE=,MjU=,NTQ=,ODM=,NzQ=,NzM=,NjI=,NTY=,NDk=,NDQ=,Njk=,Njg=,Mjk=,Mjc=,NzE=,MTc=,NTc=,NjU=,MTY=,MTA4,NjA=,NjQ=,MTk=,MTU=,MTg=,MjY=,MjQ=,MzM=,MzQ=,MzY=,NTM=,MTAz,NDU=,OTg=,OTA=,NzY=,Nzc=,OTM=,MjI=,MjM=,Mjg=,MzA=,MzU=,MzI=,NDE=,NDM=,ODA=,ODU=,NTU=,NTk=,MTAy,OTI="
}

# Pilih hanya kolom yang diinginkan
desired_columns = [
    'data.id_order_header',
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
    'data.trx_order_pilots.pilot_deploy',
    'data.trx_order_pilots.pilot_arrive',
    'data.trx_order_pilots.pilot_onboard',
    'data.trx_order_pilots.pilot_start',
    'data.trx_order_pilots.pilot_end',
    'data.trx_order_pilots.pilot_off',
    'data.company_name',
    'data.movement_type',
    'data.tug_name',
    'data.type_name'
]

simple_names = [
    'id_order_header',
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
    'pilot_deploy',
    'pilot_arrive',
    'pilot_onboard',
    'pilot_start',
    'pilot_end',
    'pilot_off',
    'company_name',
    'movement_type',
    'tug_name',
    'type_name'
]

# Daftar order IDs untuk diambil (mulai dari ID rendah dan ambil sampai tidak ada data)
start_id_file = 'last_id.txt'
if os.path.exists(start_id_file):
    with open(start_id_file, 'r') as f:
        start_id = int(f.read().strip())
else:
    start_id = 2011000
order_ids = []
current_id = start_id
consecutive_failures = 0
max_consecutive_failures = 50
max_attempts = 1000000
counter = 0
consecutive_failures = 0

while counter < max_attempts and consecutive_failures < max_consecutive_failures:
    counter += 1
    print(f"Processing ID: {current_id}")
    url = f"https://phinnisi.pelindo.co.id:9018/api/jobactivities/detail-order/{current_id}"
    print(f"Mengambil data untuk order ID: {current_id}")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        # Cek jika data ada (misalnya, jika ada id_order_header)
        if data and 'data' in data and data['data']:
            order_ids.append(current_id)
            consecutive_failures = 0  # Reset counter
            print(f"Berhasil menemukan data untuk order ID: {current_id}")
        else:
            consecutive_failures += 1
            print(f"Tidak ada data untuk order ID: {current_id}")
    else:
        consecutive_failures += 1
        print(f"Gagal mengambil data untuk order ID: {current_id} - Status: {response.status_code}")
    
    current_id += 1
    time.sleep(0.1)

print(f"Stopped after {counter} attempts, {len(order_ids)} order IDs found, consecutive failures: {consecutive_failures}")

print(f"Total order IDs ditemukan: {len(order_ids)}")

all_data = []
batch_size = 1000000
batch_count = 0

for order_id in order_ids:
    url = f"https://phinnisi.pelindo.co.id:9018/api/jobactivities/detail-order/{order_id}"
    print(f"Mengambil data untuk order ID: {order_id}")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        # Normalisasi data JSON
        df = pd.json_normalize(data)
        all_data.append(df)
        batch_count += 1
        print(f"Berhasil mengambil data untuk order ID: {order_id}")
        
        # Simpan setiap batch_size
        if batch_count % batch_size == 0:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"Data sebelum filter: {len(combined_df)}")
            
            # Filter berdasarkan approval_date dari hari berjalan -1 hingga +2 hari
            # start_date = (datetime.now().date() - timedelta(days=1))
            # end_date = (datetime.now().date() + timedelta(days=2))
            # combined_df['data.approval_date'] = pd.to_datetime(combined_df['data.approval_date'], errors='coerce').dt.date
            # combined_df = combined_df[(combined_df['data.approval_date'] >= start_date) & (combined_df['data.approval_date'] <= end_date)]
            print(f"Data setelah filter: {len(combined_df)} (filter disabled)")
            print(f"Columns in combined_df: {list(combined_df.columns)}")
            
            if not combined_df.empty:
                try:
                    print("Saving data...")
                    # Simpan semua kolom untuk test
                    if os.path.exists('job.csv'):
                        existing_df = pd.read_csv('job.csv')
                        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
                    combined_df.to_csv('job.csv', index=False)
                    print(f"Batch {batch_count // batch_size} disimpan, total data: {len(combined_df)}")
                except Exception as e:
                    print(f"Error saving: {e}")
                # Save last ID
                with open(start_id_file, 'w') as f:
                    f.write(str(current_id - 1))
                # Stop after first batch for test
                if batch_count // batch_size >= 1:
                    print("Stopping after first batch for test.")
                    break
            else:
                print(f"Batch {batch_count // batch_size} kosong setelah filter, tidak disimpan")
            all_data = []  # Reset
    else:
        print(f"Gagal mengambil data untuk order ID: {order_id} - Status: {response.status_code}")
    
    # Delay kecil untuk menghindari rate limiting
    time.sleep(0.1)

# Menggabungkan semua data
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Filter berdasarkan approval_date dari hari berjalan -1 hingga +2 hari
    # start_date = (datetime.now().date() - timedelta(days=1))
    # end_date = (datetime.now().date() + timedelta(days=2))
    # combined_df['data.approval_date'] = pd.to_datetime(combined_df['data.approval_date'], errors='coerce').dt.date
    # combined_df = combined_df[(combined_df['data.approval_date'] >= start_date) & (combined_df['data.approval_date'] <= end_date)]
    
    if not combined_df.empty:
        # Pilih hanya kolom yang diinginkan
        available_columns = [col for col in desired_columns if col in combined_df.columns]
        combined_df = combined_df[available_columns]
        
        # Rename kolom
        rename_dict = {old: new for old, new in zip(available_columns, simple_names[:len(available_columns)])}
        combined_df = combined_df.rename(columns=rename_dict)
        
        # Simpan atau append sisa
        if os.path.exists('job.csv'):
            existing_df = pd.read_csv('job.csv')
            combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
        combined_df.to_csv('job.csv', index=False)
        print(f"Data sisa berhasil disimpan ke job.csv, total: {len(combined_df)}")
    else:
        print("Data sisa kosong setelah filter, tidak disimpan")
else:
    print("Tidak ada data yang berhasil diambil.")