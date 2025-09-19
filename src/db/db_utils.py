import streamlit as st
import sqlite3
import pandas as pd

@st.cache_resource
def get_connection():
    """
    SQLite 데이터베이스에 연결합니다.
    """
    # GCS URL과 로컬 저장 경로
    gcs_url = 'https://storage.googleapis.com/webdb5/SJ_TM2360E/SJ_TM2360E.sqlite3'
    db_path = "src/db/SJ_TM2360E.sqlite3"  # 루트에 바로 저장 (경로 단순화)
    
    # os.makedirs(os.path.dirname(db_path), exist_ok=True)
    st.info("--- 2.3. 데이터베이스 파일 연결 시도 ---") # 👈 여기에 추가
    
    try:
        # 파일이 존재하지 않거나 크기가 작으면 다운로드
        if not os.path.exists(db_path) or os.path.getsize(db_path) < 10000000:
            st.info("🔄 Google Cloud Storage에서 다운로드를 시작합니다...")
            
            # requests로 파일 다운로드
            response = requests.get(gcs_url, stream=True, timeout=60)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            # 진행률 표시
            if total_size > 0:
                progress_bar = st.progress(0)
                status_text = st.empty()
            
            with open(db_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        if total_size > 0:
                            progress = downloaded_size / total_size
                            progress_bar.progress(progress)
                            status_text.text(f"다운로드 중: {downloaded_size/(1024*1024):.1f}MB / {total_size/(1024*1024):.1f}MB")
            
            # 진행률 표시 제거
            if total_size > 0:
                progress_bar.empty()
                status_text.empty()
            
            # 다운로드 검증
            if os.path.exists(db_path) and os.path.getsize(db_path) > 10000000:
                st.success(f"✅ 다운로드 완료! 파일 크기: {os.path.getsize(db_path)/(1024*1024):.1f}MB")
            else:
                st.error("❌ 다운로드된 파일이 유효하지 않습니다.")
                return None
        else:
            st.success(f"✅ 로컬 파일 발견: {os.path.getsize(db_path)/(1024*1024):.1f}MB")

        # SQLite 연결
        conn = sqlite3.connect(db_path, check_same_thread=False)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결에 실패했습니다: {e}")
        return None

def read_data_from_db(conn, table_name):
    """
    데이터베이스에서 지정된 테이블의 데이터를 읽어 DataFrame으로 반환합니다.
    """
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"테이블 '{table_name}'에서 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        return None
