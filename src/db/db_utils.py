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
    st.info("--- 2.1. get_connection() 함수 실행 ---") # 👈 여기에 추가
    """
    구글 클라우드 스토리지에서 파일을 다운로드하고 SQLite 연결을 반환합니다.
    """
    # GCS URL과 로컬 저장 경로
    gcs_url = 'https://storage.googleapis.com/webdb5/SJ_TM2360E/SJ_TM2360E.sqlite3'
    db_path = "C:\Users\samjin\test5\yhkim2\src\db\SJ_TM2360E_v3.sqlite3"  # 루트에 바로 저장 (경로 단순화)
    
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
        st.success("✅ 데이터베이스 연결 성공!") # 👈 여기에 추가
        # 연결 테스트
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;")
        test_result = cursor.fetchone()
        
        if test_result:
            st.success("✅ 데이터베이스 연결 성공")
        else:
            st.warning("⚠️ 데이터베이스에 테이블이 없습니다.")
        
        return conn
        
    except requests.exceptions.RequestException as e:
        st.error(f"❌ 네트워크 오류: {e}")
        return None
    except sqlite3.Error as e:
        st.error(f"❌ SQLite 오류: {e}")
        return None
    except Exception as e:
        st.error(f"❌ 예상치 못한 오류: {e}")
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

def show_database_info(conn):
    """
    데이터베이스 정보를 표시합니다.
    """
    if conn is None:
        return
        
    try:
        # 테이블 목록
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
        
        if len(tables) > 0:
            st.info(f"📊 데이터베이스에 {len(tables)}개의 테이블이 있습니다")
            
            with st.expander("테이블 정보 보기"):
                for table_name in tables['name']:
                    try:
                        count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name};", conn)
                        row_count = count.iloc[0]['count']
                        st.write(f"• **{table_name}**: {row_count:,}행")
                    except:
                        st.write(f"• **{table_name}**: 조회 실패")
        else:
            st.warning("⚠️ 테이블을 찾을 수 없습니다.")
                
    except Exception as e:
        st.error(f"데이터베이스 정보 조회 실패: {e}")