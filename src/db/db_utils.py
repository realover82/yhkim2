import streamlit as st
import sqlite3
import pandas as pd
import os
import requests
import warnings

# 경고 무시
warnings.filterwarnings('ignore')

@st.cache_resource
def get_connection():
    """
    구글 클라우드 스토리지에서 파일을 다운로드하고 SQLite 연결을 반환합니다.
    """
    # GCS에서 다운로드할 파일 URL과 로컬 저장 경로를 정의합니다.
    # YOUR_GCS_URL 부분을 2단계에서 복사한 URL로 교체하세요!
    gcs_url = 'https://storage.googleapis.com/webdbd5/SJ_TM2360E.sqlite3' # 👈 여기에 당신의 URL을 붙여넣으세요
    db_path = "src/db/SJ_TM2360E_v2.sqlite3"

    # src/db 디렉터리가 없으면 생성합니다.
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # 1단계: 파일이 존재하지 않거나 유효하지 않을 경우 다운로드를 시도합니다.
    # 파일 크기가 10MB 이상인지 검증하여 유효한 파일인지 확인합니다.
    if not os.path.exists(db_path) or os.path.getsize(db_path) < 10000000:
        st.info("🔄 유효한 로컬 파일이 없습니다. Google Cloud Storage에서 다운로드를 시작합니다...")
        try:
            # requests 라이브러리를 사용하여 파일을 다운로드합니다.
            with requests.get(gcs_url, stream=True) as r:
                r.raise_for_status() # HTTP 오류 발생 시 예외를 일으킵니다.
                
                download_progress = st.progress(0)
                downloaded_size = 0
                total_size = int(r.headers.get('content-length', 0))

                with open(db_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        download_progress.progress(downloaded_size / total_size)
            
            st.success("✅ GCS 다운로드 완료!")
            
            if os.path.exists(db_path) and os.path.getsize(db_path) > 10000000:
                pass # 다운로드 성공
            else:
                st.error("❌ 다운로드 실패 또는 파일 크기가 유효하지 않습니다.")
                st.stop()
                return None
        except Exception as e:
            st.error(f"❌ GCS 다운로드 중 오류 발생: {e}")
            st.stop()
            return None
    else:
        st.success(f"✅ 로컬에서 유효한 데이터베이스 파일 발견: {db_path}")

    # 2단계: 다운로드한 파일에 연결을 시도합니다.
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        return conn
    except Exception as e:
        st.error(f"❌ 데이터베이스 연결에 실패했습니다: {e}")
        st.stop()
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