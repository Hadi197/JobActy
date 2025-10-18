import requests
import json
import pandas as pd
import time
import os
import subprocess
from datetime import datetime, timedelta


def save_batch(batch_df, filename='job.csv'):
    """Simpan batch ke CSV lalu commit & push ke GitHub."""
    if batch_df.empty:
        print(f"[{datetime.now()}] ⚠️ Batch kosong, tidak disimpan.")
        return

    file_exists = os.path.exists(filename)

    # Simpan ke CSV
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        batch_df.to_csv(f, header=not file_exists, index=False)
        f.flush()
        os.fsync(f.fileno())

    print(f"[{datetime.now()}] ✅ Batch disimpan ({len(batch_df)} baris) ke {filename}")

    print(f"[{datetime.now()}] 🔄 Commit & push batch ke GitHub...")

    try:
        # Configure git
        subprocess.run(['git', 'config', '--global', 'user.name', 'github-actions[bot]'], check=True)
        subprocess.run(['git', 'config', '--global', 'user.email', 'github-actions[bot]@users.noreply.github.com'], check=True)

        # Add files
        subprocess.run(['git', 'add', '-f', 'job.csv', 'last_id.txt'], check=True)

        # Commit with proper date
        commit_msg = f"Auto commit batch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        result = subprocess.run(['git', 'commit', '-m', commit_msg], capture_output=True, text=True)
        if result.returncode != 0:
            if 'nothing to commit' in result.stdout or 'nothing to commit' in result.stderr:
                print(f"[{datetime.now()}] ⚠️ No changes to commit")
                return
            else:
                print(f"[{datetime.now()}] ❌ Commit failed: {result.stderr}")
                return

        # Push
        repo_url = os.environ.get('REPO_URL', '').strip()
        if repo_url:
            push_cmd = ['git', 'push', repo_url, 'HEAD:main']
        else:
            push_cmd = ['git', 'push', 'origin', 'main']

        result = subprocess.run(push_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"[{datetime.now()}] ⚠️ Push failed: {result.stderr}")
            # Try pull rebase and push again
            subprocess.run(['git', 'pull', '--rebase'], check=False)
            result = subprocess.run(push_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"[{datetime.now()}] ❌ Push still failed after rebase: {result.stderr}")
            else:
                print(f"[{datetime.now()}] ✅ Push successful after rebase")
        else:
            print(f"[{datetime.now()}] ✅ Push successful")

    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now()}] ❌ Git command failed: {e}")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Unexpected error: {e}")

# Header dengan access-token
headers = {
    "accept": "application/json, text/plain, */*",
    "access-token": "eyJhbGciOiJIUzI1NiJ9.VTJGc2RHVmtYMStVTG5GVWJtcWVRWGFFWStlNVRoNEQ3UGFJbmRrTDMyMndnd1Z0ZEkzK0p6Q2NrNEZQQTg4d2phUkJsUlRla2tkU0JqUGJodGlsMDUybVlWVzJJVXVxTHJlcVJ1aU5jV1VKOE9tK1kzYUNqRmVzVXUrb0xUSzczNkc0TXhmbTdMakQ1WVpnMFlkRGdHZWJNTGtyRFlyUkwvY1NFWkNvakVIV2lvbEQ1U0RHUTcvZWdaMG5LZVdJRGxDOWsrQ0dGN3l5aWQrUXVJd1VCcGUyUDJuM28zVnR4ZU4vZlNnak1zcHVySllrZGdJbjhSSkFRQjVUdU9OblFSbGJnSHRvY3JvUnRwVzBpS1JXOWw0bXBHS2NPZVdPNnJqN3kwNzhlSTNZQ0lySUZkY0labWdnSHpxcHZxWmtpQXBvUFN5Qi9yTzBwYk1wLzhFQnlVRC9XUHd4RlI0NDQ3UEE2UDNUT1QzSWZKdW9xY2RPTHJpNXJNc2VEU2JTbUdiK29aSmpxcjZsWXJldXVSdVliQk9EQ3hjdnFEOXRPMnlyelFKRGx3ZHY5djNLUTFFd2IzUFg2Q1lzQXExc1BvVHk0cGlObmZmUjV1UWRUZnV2M1ByS2k4QVFabFZhT0FBV1I2ZGp4MzFRS3ZDWTFxemhxOVhGNTFzZk84UnpicWV1V0k2K3liVy91Q20zNWxCWlR0YSt5Nk95ZHVWZWcvU1Bsck1pMVcrTnJQVjhZMHpyTXlvZlFCdFUvWnhJNGZFdnowMEdXaU1BWThiWTJaSzJWaExqUEh2Q1hKT21ENXViTzREcnMzeUwwQ01HcE83SitWNmpZeU9jZHFqUlhDRlBHMXhWZVJzRmhDZjV3TmFNQVRIZmJSTjJtc0FkZ0JaVWM4TW4xMHJOTnE5azFVOC83L3psSUZ6T1Y0UzZNTUsvVitBV0VhUXFJSS8xamdYdDBWaHA4Si83eHM4andMR3djQ2MweDEyWVNFRWVzVE92SEo0azZSUmlabXdFL2tXNWMwL1pEZ0hxb3lCYXB0WDBEVENidWh5SlQzTzVzWGYyc0JFWmxsRU9aMjhTV05GYUhqUkNLa3VpWDljYStRNXNpOUh0NWVtT2hnenorcEpXU015R24rVS8xclhqU2F2YW8wc3BYTWVTK29Zd0pTWlVLZVVReXIrY1pMNFo5YWVaZEZDSFhZNzA5alNVRnJSTmRuZ0J2NUticXlXRDFHaHpDT1dabllTbENiZzlUWHBJOE5jSy9CZ0phUkZxMll1ZUZJc3loRjBsME5iTGZKNnZuSng3OGJUa24vaEk4ckNzY1EvVkRYL3pYNVBpWWFGOFZMWVJCNmN2MXlwQWRITzdEbGlEbnFiRkdsTFV1WnYwUmNPd2dzV2U5UE8vZ25NT1dMZ0MyTTVjczZMdlRCK0RNMjgrcVFMUUlHSWtuZC96c0c5L2J6UjhNNjRSbi8zQWZldE9ieE5wQk5nNW5YZzFmNDRnVWhRZ3RSdHBwY3R4ZS84Mi85ZitUeHVvRDdOMVErY1d4NWpFT2hFNXo3Um5ESXgva1g2L0dteFo1UXVGaUlCczkvK2lxSHlobEM3NWNzUnh4SjBkcGRPTzhKcmdsT0pTUDBRYWZRU2ovMFh5b2dRejV2Yk9DelpJNUx3Wlk2QUFMUjRIeHVmYXErdE82eEd5RzdkdFBaZDBuek5jY3A4K25BeUh6ZkxMbHR2S0RzUHVEaUhOd011enBKeEVTZ1ZaVEdVYStsb0hUME5KMzd3cS9PNS9GeWtSdDBNMWIrcWJiVkRlbkxiVlZPcjMwa3ZJcWJYdUx2NC8xVy9ZQmtrRitXQUpybUFOOTh5QXE3R1dndGJuMjlQYll1bGIxd3o5NmhLNjVOK3JIdUxnNTZsL1NDeWR6MyttcUIxK3JldkY5N3daM3hkek5TM29pNlloOGp0Sk9BaEN3Vlk3aUZGQVJoek9nQVZudFhOZjNGdk1lOG9VZldJbWRTZnZTdXNLQ01FY09uQ2tpcDhMb3dpVVEyRXZXUGo4djZTc1ZURnBTS0tXV20vcWtaTUhXa3I3VUZNWVROQ1Z5TXhjVGxITHlZRXR5b1JaYVppNG9mVWllak9aaytKeTVjWC90alBkL0pncFlqZnAySTZHcHkvOGdkdTRhZjQ2aFpEd2crZjdiZXYva2J6RkcrVVR2WnFyQ2ZmWXh0VlpBWHd0aUoySDIyM0wvWUZ2UVR5VlFWdFRoenRsRFFsWndkNlRPUjZ3dkxzQmVzWmp6N0RlM1RNUTVnSnpFaHNEeHJpMkxmV3ZjdUx5RFYrUHl5c3ZQNWxvUVozZnU5aDRaTWRLZXhkbGozT2dEbytJYzErTVpXYmtmQ25NeUlUMUZTM2M5Z052Zk1PL1I1bDhadks5VTRKaUlsRWkxb056V0FNZlptRHZ3dmlJbm9nNEp5ZjZWUkdMb3VQRkE5K2tEZzRFNFZKcElIQUE3ekJRM1llMDE3TitOS2d5RXRqcjV6M2FCd0tINGxBZ0pvcnhsMFRXNm84ZGNKWlNoRVUwc1E2ZGdsWlNvazlhd055dDNtbE1pMFVJNVhDODViWWpOK3RiTkJXa0FiRGZGRVFESTVpTStPSDU3YVplZzUyOUZIMVd1MXV6dko0d1dpUS96TGpmNVdudEFCWWl3TURuN01zdjFVaDhzbzRFVC9yanBaNzFCTS9Ga0hqUkIrL0d4NGdnSVlPMTBLT1c1bWFMdE9QU1VPRC9oNTdpV25Ob2gzYXovamszY1l4c0lHNm5sb1V5eng0L1Z0dGR3MUxFODQzUE02U3JURHVRSitEOGg4SkN0eXFnQXFwMXRiQjR4bmN5WVkvKzJuWnBGSmdzSUp1QW1UNWVyd2FzTzlla1hHaFd1bkV2NmtNcGNHVmliblR6OFhRMHRiYXNHMXdIT1pOMDMzMHpPWHpvZVZac2tVSExzSHpVMEJ5ZE1ka3k3Sm50NjJ6MC9nZUYveTZJdVhoZ1loQWxzNjRXOWMyenBISkZHdStjL0Jzc2RQODV2eU5GL3FnSVRzTHMxTlFlV2FncmVPNXJPZjFwSTViazlxcFpSK2I1aXAySW9aYnNzMCtPdHozMVJTMGMvazZvTkRtd0EvdTBLYjZDL2hDY0JqQUp1cVB4Y3M2YVNkMU53UzA2U3plaXZkVVBoazFjQWZaUk43ZHRRK2Y0dGR0SFdhNkJqRmZvL3ZiRFlKb1A.gTMAZPqNLRYWlw39P3wq0eVsxzX5YYhYrZvARVNyPVE",
    "sub-branch": "MTQy,ODI=,NDc=,NTE=,NTI=,ODg=,ODc=,ODY=,Mzc=,NDA=,NTA=,NDg=,ODQ=,Mzg=,MzE=,NjM=,Mzk=,MTA0,MjA=,Mg==,MTEy,NA==,NQ==,Ng==,OQ==,MTA=,MTE=,MTI=,MTM=,MTQ=,MQ==,MTEx,MTEw,MTA2,MTA1,Mw==,NzU=,NzI=,NTg=,NjY=,MTAx,NjE=,ODE=,MjU=,NTQ=,ODM=,NzQ=,NzM=,NjI=,NTY=,NDk=,NDQ=,Njk=,Njg=,Mjk=,Mjc=,NzE=,MTc=,NTc=,NjU=,MTY=,MTA4,NjA=,NjQ=,MTk=,MTU=,MTg=,MjY=,MjQ=,MzM=,MzQ=,MzY=,NTM=,MTAz,NDU=,OTg=,OTA=,NzY=,Nzc=,OTM=,MjI=,MjM=,Mjg=,MzA=,MzU=,MzI=,NDE=,NDM=,ODA=,ODU=,NTU=,NTk=,MTAy,OTI="
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
    'pilot_deploy',
    'pilot_arrive',
    'pilot_onboard',
    'pilot_start',
    'pilot_end',
    'pilot_off'
]

if not os.path.exists("job.csv"):
    pd.DataFrame(columns=simple_names).to_csv("job.csv", index=False)

# Hapus job.csv lama untuk memulai fresh setiap eksekusi
if os.path.exists("job.csv"):
    os.remove("job.csv")
    print("Menghapus job.csv lama untuk memulai fresh.")
    pd.DataFrame(columns=simple_names).to_csv("job.csv", index=False)

# Load existing IDs hanya untuk dedupe in-memory selama run ini (kosong karena fresh)
existing_ids = set()

# Daftar order IDs untuk diambil (mulai dari ID rendah dan ambil sampai tidak ada data)
start_id_file = 'last_id.txt'
start_id = 2011000  # Selalu mulai dari awal untuk mendapatkan data terbaru
with open(start_id_file, 'w') as f:
    f.write(str(start_id))  # Update last_id.txt ke 2011000
current_id = start_id
consecutive_failures = 0
max_consecutive_failures = 15
max_attempts = 1000000
counter = 0
all_data = []
batch_size = 50
batch_count = 0

while counter < max_attempts and consecutive_failures < max_consecutive_failures:
    counter += 1
    print(f"Processing ID: {current_id}")
    url = f"https://phinnisi.pelindo.co.id:9018/api/jobactivities/detail-order/{current_id}"
    print(f"Mengambil data untuk order ID: {current_id}")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request error: {e}")
        consecutive_failures += 1
        current_id += 1
        continue
    
    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            print("⚠️ Response bukan JSON, skip.")
            data = None
        # Cek jika data ada (misalnya, jika ada id_order_header)
        if data and 'data' in data and data['data']:
            # Normalisasi data JSON langsung
            df = pd.json_normalize(data)
            # Add missing columns with None
            for col in desired_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[desired_columns]
            rename_dict = dict(zip(desired_columns, simple_names))
            df.rename(columns=rename_dict, inplace=True)
            
            # Check for duplicates
            if 'id_order_header' in df.columns and not df.empty:
                order_id = df['id_order_header'].iloc[0]
                if order_id in existing_ids:
                    print(f"Duplicate ID {order_id} found, skipping.")
                    consecutive_failures += 1
                    current_id += 1
                    continue
                else:
                    existing_ids.add(order_id)
            
            all_data.append(df)
            batch_count += 1
            consecutive_failures = 0  # Reset counter
            print(f"Berhasil menemukan dan menyimpan data untuk order ID: {current_id}")
            
            # Simpan setiap batch_size
            if len(all_data) >= batch_size:
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
                        print("Preparing to save batch...")

                        # Read existing IDs on disk to avoid duplicates (in case file changed externally)
                        existing_on_disk = set()
                        if os.path.exists('job.csv'):
                            try:
                                existing_on_disk = set(pd.read_csv('job.csv', usecols=['id_order_header'])['id_order_header'].dropna().astype(int))
                            except Exception:
                                existing_on_disk = set()

                        # Filter out IDs already present on disk
                        if 'id_order_header' in combined_df.columns:
                            combined_df['id_order_header'] = combined_df['id_order_header'].astype(int)
                            before_count = len(combined_df)
                            combined_df = combined_df[~combined_df['id_order_header'].isin(existing_on_disk)]
                            filtered_count = len(combined_df)
                            print(f"Filtered {before_count - filtered_count} duplicate rows based on existing job.csv")

                        if not combined_df.empty:
                            save_batch(combined_df, filename='job.csv')
                            # Update in-memory existing_ids with what we just saved
                            try:
                                saved_ids = set(combined_df['id_order_header'].dropna().astype(int))
                                existing_ids.update(saved_ids)
                            except Exception:
                                pass

                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Batch disimpan (ID terakhir: {current_id})")
                            # Clear all_data after saving
                            all_data = []
                            with open(start_id_file, 'w') as f:
                                f.write(str(current_id))
                    except Exception as e:
                        print(f"Error saving data: {e}")
        else:
            consecutive_failures += 1
            print(f"Tidak ada data untuk order ID: {current_id}")
    else:
        consecutive_failures += 1
        print(f"Gagal mengambil data untuk order ID: {current_id} - Status: {response.status_code}")
    
    current_id += 1
    time.sleep(0.3)  # 3 request per detik (aman)

print(f"Stopped after {counter} attempts, {len(all_data)} data in memory, consecutive failures: {consecutive_failures}")

# Simpan last_id
with open(start_id_file, 'w') as f:
    f.write(str(current_id))


# Simpan data sisa jika ada
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"Data sisa sebelum filter: {len(combined_df)}")
    
    # Filter berdasarkan approval_date dari hari berjalan -1 hingga +2 hari
    # start_date = (datetime.now().date() - timedelta(days=1))
    # end_date = (datetime.now().date() + timedelta(days=2))
    # combined_df['data.approval_date'] = pd.to_datetime(combined_df['data.approval_date'], errors='coerce').dt.date
    # combined_df = combined_df[(combined_df['data.approval_date'] >= start_date) & (combined_df['data.approval_date'] <= end_date)]
    print(f"Data sisa setelah filter: {len(combined_df)} (filter disabled)")
    print(f"Columns in combined_df: {list(combined_df.columns)}")
    
    if not combined_df.empty:
        try:
            print("Preparing to save remaining data...")

            # Read existing IDs on disk to avoid duplicates
            existing_on_disk = set()
            if os.path.exists('job.csv'):
                try:
                    existing_on_disk = set(pd.read_csv('job.csv', usecols=['id_order_header'])['id_order_header'].dropna().astype(int))
                except Exception:
                    existing_on_disk = set()

            if 'id_order_header' in combined_df.columns:
                combined_df['id_order_header'] = combined_df['id_order_header'].astype(int)
                before_count = len(combined_df)
                combined_df = combined_df[~combined_df['id_order_header'].isin(existing_on_disk)]
                filtered_count = len(combined_df)
                print(f"Filtered {before_count - filtered_count} duplicate rows in remaining data")

            if not combined_df.empty:
                save_batch(combined_df, filename='job.csv')
                try:
                    saved_ids = set(combined_df['id_order_header'].dropna().astype(int))
                    existing_ids.update(saved_ids)
                except Exception:
                    pass

                print(f"Data sisa berhasil disimpan ke job.csv, total: {len(combined_df)}")
            else:
                print("Data sisa kosong setelah filter, tidak disimpan")
        except Exception as e:
            print(f"Error saving remaining data: {e}")
    else:
        print("Data sisa kosong setelah filter, tidak disimpan")
else:
    print("Tidak ada data yang berhasil diambil.")
