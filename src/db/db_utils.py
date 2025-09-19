import streamlit as st
import sqlite3
import pandas as pd
import os
import gdown

@st.cache_resource
def get_connection():
    """
    Google 드라이브에서 파일을 다운로드하고 SQLite 데이터베이스에 연결합니다.
    """
    # Google 드라이브 파일 ID와 로컬 저장 경로를 정의합니다.
    file_id = '1srULKQgBNiNTWHTWatpKS0faeJ2xD8SN' # <<-- 이 부분을 수정했습니다!
    db_path = "src/db/SJ_TM2360E_v2.sqlite3"

    # 파일이 로컬에 없으면 다운로드합니다.
    if not os.path.exists(db_path):
        st.info("Google 드라이브에서 데이터베이스 파일을 다운로드 중...")
        try:
            # src/db 디렉터리가 없으면 생성합니다.
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            gdown.download(f'https://drive.google.com/uc?id={file_id}', db_path, quiet=False)
            st.success("다운로드 완료!")
        except Exception as e:
            st.error(f"Google 드라이브에서 파일 다운로드 중 오류가 발생했습니다: {e}")
            return None

    try:
        # 다운로드된 파일에 연결합니다.
        conn = sqlite3.connect(db_path, check_same_thread=False)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결에 실패했습니다: {e}")
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