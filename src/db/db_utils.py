import streamlit as st
import sqlite3
import pandas as pd
import os
import gdown
import warnings

# 경고 무시
warnings.filterwarnings('ignore')

@st.cache_resource
def get_connection():
    """
    Google 드라이브에서 파일을 다운로드하고 SQLite 연결을 반환합니다.
    """
    # Google Drive 파일 ID와 로컬 저장 경로를 정의합니다.
    file_id = '1srULKQgBNiNTWHTWatpKS0faeJ2xD8SN'
    db_path = "src/db/SJ_TM2360E.sqlite3"

    # src/db 디렉터리가 없으면 생성합니다.
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # 1단계: 파일이 존재하지 않거나 유효하지 않을 경우 다운로드를 시도합니다.
    if not os.path.exists(db_path) or os.path.getsize(db_path) < 10000000: # 10MB 이상으로 검증
        st.info("🔄 유효한 로컬 파일이 없습니다. Google Drive에서 다운로드를 시작합니다...")
        try:
            gdown.download(f'https://drive.google.com/uc?id={file_id}', db_path, quiet=False, timeout=6000)
            
            if os.path.exists(db_path) and os.path.getsize(db_path) > 10000000:
                st.success("✅ Google Drive 다운로드 완료!")
            else:
                st.error("❌ 다운로드 실패 또는 파일 크기가 유효하지 않습니다.")
                st.stop()
                return None
        except Exception as e:
            st.error(f"❌ Google Drive 다운로드 중 오류 발생: {e}")
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