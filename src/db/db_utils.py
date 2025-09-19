import streamlit as st
import sqlite3
import pandas as pd
import os
import requests
import gdown # gdown 라이브러리를 추가합니다.

# 경고 무시
import warnings
warnings.filterwarnings('ignore')

def download_file_with_progress(url, destination):
    """
    진행률을 표시하며 파일 다운로드
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, stream=True, timeout=600)
        
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            
            # 진행률 표시
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            downloaded = 0
            with open(destination, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            progress = downloaded / total_size
                            progress_bar.progress(min(progress, 1.0))
                            status_text.text(f"다운로드 중: {downloaded/(1024*1024):.1f}MB / {total_size/(1024*1024):.1f}MB")
            
            progress_bar.empty()
            status_text.empty()
            return True
        else:
            return False
            
    except Exception as e:
        st.error(f"다운로드 오류: {e}")
        return False

def download_large_file_from_drive(file_id, destination):
    """
    Google Drive 대용량 파일 전용 다운로드
    """
    try:
        URL = "https://drive.google.com/uc?export=download"
        
        session = requests.Session()
        
        response = session.get(URL, params={'id': file_id}, stream=True)
        token = get_confirm_token(response)
        
        if token:
            params = {'id': file_id, 'confirm': token}
            response = session.get(URL, params=params, stream=True)
        
        if response.status_code == 200:
            # 진행률 표시
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(destination, "wb") as f:
                for chunk in response.iter_content(32768):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            progress = downloaded / total_size
                            progress_bar.progress(min(progress, 1.0))
                            status_text.text(f"다운로드 중: {downloaded/(1024*1024):.1f}MB / {total_size/(1024*1024):.1f}MB")
            
            progress_bar.empty()
            status_text.empty()
            return True
        else:
            return False
            
    except Exception as e:
        st.error(f"대용량 다운로드 실패: {e}")
        return False

def get_confirm_token(response):
    """
    Google Drive 확인 토큰 추출
    """
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value
    return None

def read_data_from_db(conn, table_name):
    """
    데이터베이스에서 지정된 테이블의 데이터를 읽어 DataFrame으로 반환합니다.
    """
    if conn is None:
        return pd.DataFrame()
        
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"테이블 '{table_name}'에서 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

# 추가: 데이터베이스 정보 표시 함수
def show_database_info(conn):
    """
    데이터베이스 정보를 표시합니다.
    """
    if conn is None:
        return
        
    try:
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
        st.info(f"데이터베이스에 {len(tables)}개의 테이블이 있습니다: {', '.join(tables['name'].tolist())}")
        
        for table_name in tables['name']:
            try:
                count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name};", conn)
                row_count = count.iloc[0]['count']
                st.info(f"- {table_name}: {row_count:,}행")
            except:
                continue
                
    except Exception as e:
        st.error(f"데이터베이스 정보 조회 실패: {e}")

# ---
# get_connection 함수가 수정된 부분입니다.
# ---

@st.cache_resource
def get_connection():
    """
    데이터베이스 파일을 확보하고 SQLite 연결을 반환합니다.
    - 1단계: Google Drive에서 다운로드 시도 (클라우드 환경 최우선)
    - 2단계: 로컬에 존재하는 파일 탐색 (개발용 백업)
    """
    
    # 1단계: Google Drive에서 다운로드 시도
    file_id = '1srULKQgBNiNTWHTWatpKS0faeJ2xD8SN'
    download_path = "src/db/SJ_TM2360E.sqlite3"
    
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    
    download_success = False
    try:
        st.info("🔄 Google Drive에서 데이터베이스 다운로드를 시도합니다...")
        # gdown 라이브러리 사용
        gdown.download(f'https://drive.google.com/uc?id={file_id}', download_path, quiet=False)
        
        if os.path.exists(download_path) and os.path.getsize(download_path) > 10000000:
            st.success("✅ Google Drive 다운로드 완료!")
            download_success = True
        else:
            st.error("❌ 다운로드 실패 또는 파일 크기가 유효하지 않습니다.")
    except Exception as e:
        st.error(f"❌ Google Drive 다운로드 중 오류 발생: {e}")

    # 2단계: 연결 시도
    # 다운로드가 성공했거나, 로컬 파일이 존재할 경우
    if download_success:
        try:
            conn = sqlite3.connect(download_path, check_same_thread=False)
            return conn
        except Exception as e:
            st.error(f"❌ 다운로드된 파일에 연결 실패: {e}")
            return None
    else:
        st.warning("⚠️ Google Drive 다운로드에 실패했습니다. 로컬 파일을 찾습니다...")
        # 로컬 파일 탐색 (기존 코드의 로직)
        local_paths = [
            "src/db/SJ_TM2360E.sqlite3",
            "SJ_TM2360E.sqlite3",
        ]
        
        for path in local_paths:
            if os.path.exists(path) and os.path.getsize(path) > 10000000:
                st.success(f"✅ 로컬 데이터베이스 파일 발견: {path}")
                try:
                    conn = sqlite3.connect(path, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;")
                    cursor.fetchone()
                    return conn
                except Exception as e:
                    st.warning(f"로컬 파일 {path} 연결 실패: {e}")
                    continue
        
    st.error("❌ 모든 시도가 실패했습니다. 데이터베이스 연결을 할 수 없습니다.")
    return None