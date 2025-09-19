import streamlit as st
import sqlite3
import pandas as pd
import os
import requests
import tempfile
from urllib.parse import urlparse

@st.cache_resource
def get_connection():
    """
    여러 방법으로 데이터베이스 파일을 확보하고 SQLite 연결을 반환합니다.
    """
    
    # 방법 1: 로컬 파일이 있는지 확인 (Git LFS 또는 로컬 개발용)
    local_paths = [
        "src/db/SJ_TM2360E.sqlite3",
        # "src/db/SJ_TM2360E_v2.sqlite3", 
        "SJ_TM2360E.sqlite3",
        # "SJ_TM2360E_v2.sqlite3"
    ]
    
    for path in local_paths:
        if os.path.exists(path) and os.path.getsize(path) > 10000000:  # 10MB 이상
            st.success(f"✅ 로컬 데이터베이스 파일 발견: {path}")
            try:
                conn = sqlite3.connect(path, check_same_thread=False)
                # 연결 테스트
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;")
                cursor.fetchone()
                return conn
            except Exception as e:
                st.warning(f"로컬 파일 {path} 연결 실패: {e}")
                continue
    
    # 방법 2: Google Drive에서 다운로드
    st.info("🔄 로컬 파일이 없습니다. Google Drive에서 다운로드를 시도합니다...")
    
    # Google Drive 파일 정보
    file_id = '1srULKQgBNiNTWHTWatpKS0faeJ2xD8SN'
    db_path = "downloaded_database.sqlite3"
    
    try:
        success = download_from_google_drive(file_id, db_path)
        if success:
            conn = sqlite3.connect(db_path, check_same_thread=False)
            return conn
        else:
            st.error("Google Drive 다운로드 실패")
            return None
            
    except Exception as e:
        st.error(f"데이터베이스 연결 실패: {e}")
        return None

def download_from_google_drive(file_id, destination):
    """
    Google Drive에서 대용량 파일 다운로드 (여러 방법 시도)
    """
    
    # 방법 1: 직접 다운로드 URL
    urls_to_try = [
        f"https://drive.google.com/uc?export=download&id={file_id}",
        f"https://drive.google.com/file/d/{file_id}/view?usp=sharing",
        f"https://drive.usercontent.google.com/download?id={file_id}&export=download"
    ]
    
    for i, url in enumerate(urls_to_try, 1):
        st.info(f"다운로드 방법 {i} 시도 중...")
        
        try:
            if download_file_with_progress(url, destination):
                # 파일 크기 검증
                if os.path.exists(destination) and os.path.getsize(destination) > 50000000:  # 50MB 이상
                    st.success(f"✅ 다운로드 완료! 파일 크기: {os.path.getsize(destination)/(1024*1024):.1f} MB")
                    return True
                else:
                    st.warning("다운로드된 파일이 너무 작습니다. 다음 방법을 시도합니다.")
                    if os.path.exists(destination):
                        os.remove(destination)
                        
        except Exception as e:
            st.warning(f"방법 {i} 실패: {e}")
            continue
    
    # 방법 2: 세션을 사용한 다운로드 (대용량 파일용)
    st.info("대용량 파일 다운로드 방법을 시도합니다...")
    try:
        return download_large_file_from_drive(file_id, destination)
    except Exception as e:
        st.error(f"모든 다운로드 방법 실패: {e}")
        return False

def download_file_with_progress(url, destination):
    """
    진행률을 표시하며 파일 다운로드
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, stream=True, timeout=30)
        
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
        # 테이블 목록
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
        st.info(f"데이터베이스에 {len(tables)}개의 테이블이 있습니다: {', '.join(tables['name'].tolist())}")
        
        # 각 테이블의 행 수 확인
        for table_name in tables['name']:
            try:
                count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name};", conn)
                row_count = count.iloc[0]['count']
                st.info(f"- {table_name}: {row_count:,}행")
            except:
                continue
                
    except Exception as e:
        st.error(f"데이터베이스 정보 조회 실패: {e}")