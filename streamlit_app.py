import streamlit as st
import pandas as pd
from datetime import datetime, date
import warnings
import sys
import os

# 현재 파일의 절대 경로를 기준으로 프로젝트 루트 디렉토리를 찾습니다.
project_root = os.path.dirname(os.path.abspath(__file__))


# src 폴더를 Python path에 추가
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# 프로젝트 루트도 추가 (혹시 모를 경우를 대비)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 에러 처리를 위한 플래그
modules_loaded = False

# 경로 디버깅 코드
st.info("--- 경로 디버깅 시작 ---")
src_path_check = os.path.join(project_root, 'src')
st.write(f"✅ 프로젝트 루트: {project_root}")
st.write(f"✅ src 폴더 경로: {src_path_check}")
st.write(f"✅ src 폴더 존재 여부: {os.path.exists(src_path_check)}")

db_utils_path = os.path.join(src_path_check, 'db', 'db_utils.py')
st.write(f"✅ db_utils.py 파일 경로: {db_utils_path}")
st.write(f"✅ db_utils.py 파일 존재 여부: {os.path.exists(db_utils_path)}")

st.info("--- 경로 디버깅 완료 ---")
# --- 여기까지 추가 ---

# 프로젝트 내부 모듈을 import 합니다.
try:
    from db.db_utils import get_connection
    # # 수정 후 (임시 테스트)
    # import sqlite3
    # def get_connection():
    #     st.info("테스트: 간단한 메모리 DB 연결")
    #     return sqlite3.connect(':memory:')
    
    from services.analysis_service import analyze_data
    from utils.ui_helpers import display_analysis_result, display_data_views
    modules_loaded = True
    st.success("✅ 모든 모듈이 성공적으로 로드되었습니다.")
except (ImportError, ModuleNotFoundError) as e:
    st.error(f"❌ 모듈 로드 실패: {e}")
    
    # 디버그 정보 출력
    st.info("**디버그 정보:**")
    st.write(f"프로젝트 루트: {project_root}")
    st.write(f"src 경로: {src_path}")
    
    # 폴더 구조 확인
    st.info("**폴더 구조 확인:**")
    if os.path.exists(src_path):
        st.write("✅ src 폴더가 존재합니다.")
        
        # 하위 폴더 확인
        for subfolder in ['db', 'services', 'utils']:
            subfolder_path = os.path.join(src_path, subfolder)
            if os.path.exists(subfolder_path):
                st.write(f"✅ src/{subfolder} 폴더가 존재합니다.")
                
                # __init__.py 파일 확인
                init_file = os.path.join(subfolder_path, '__init__.py')
                if os.path.exists(init_file):
                    st.write(f"✅ src/{subfolder}/__init__.py 파일이 존재합니다.")
                else:
                    st.write(f"❌ src/{subfolder}/__init__.py 파일이 없습니다.")
                    
                # 주요 파일들 확인
                if subfolder == 'db':
                    db_utils_file = os.path.join(subfolder_path, 'db_utils.py')
                    if os.path.exists(db_utils_file):
                        st.write(f"✅ src/db/db_utils.py 파일이 존재합니다.")
                    else:
                        st.write(f"❌ src/db/db_utils.py 파일이 없습니다.")
                        
            else:
                st.write(f"❌ src/{subfolder} 폴더가 없습니다.")
    else:
        st.write("❌ src 폴더가 존재하지 않습니다.")
        
    st.info("""
    **해결 방법:**
    1. 폴더 구조를 다음과 같이 만드세요:
       ```
       프로젝트/
       ├── streamlit_app.py
       └── src/
           ├── __init__.py
           ├── db/
           │   ├── __init__.py
           │   └── db_utils.py
           ├── services/
           │   ├── __init__.py
           │   └── analysis_service.py
           └── utils/
               ├── __init__.py
               └── ui_helpers.py
       ```
    2. 모든 __init__.py 파일을 생성하세요 (빈 파일)
    3. 앱을 다시 실행하세요
    """)

warnings.filterwarnings('ignore')

# 세션 상태 초기화
def initialize_session_state():
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = {key: None for key in ['pcb', 'fw', 'rftx', 'semi', 'func']}
    if 'analysis_data' not in st.session_state:
        st.session_state.analysis_data = {key: None for key in ['pcb', 'fw', 'rftx', 'semi', 'func']}
    if 'analysis_time' not in st.session_state:
        st.session_state.analysis_time = {key: None for key in ['pcb', 'fw', 'rftx', 'semi', 'func']}
    if 'jig_col_mapping' not in st.session_state:
        st.session_state.jig_col_mapping = {
            'pcb': 'PcbMaxIrPwr',
            'fw': 'FwPC',
            'rftx': 'RfTxPC',
            'semi': 'SemiAssyMaxBatVolt',
            'func': 'BatadcPC',
        }
    if 'show_line_chart' not in st.session_state:
        st.session_state.show_line_chart = {}
    if 'show_bar_chart' not in st.session_state:
        st.session_state.show_bar_chart = {}
    if 'analysis_status' not in st.session_state:
        st.session_state.analysis_status = {
            key: {'analyzed': False} for key in ['pcb', 'fw', 'rftx', 'semi', 'func']
        }
    if 'snumber_search' not in st.session_state:
        st.session_state.snumber_search = {
            'pcb': {'results': pd.DataFrame(), 'show': False},
            'fw': {'results': pd.DataFrame(), 'show': False},
            'rftx': {'results': pd.DataFrame(), 'show': False},
            'semi': {'results': pd.DataFrame(), 'show': False},
            'func': {'results': pd.DataFrame(), 'show': False},
        }
    if 'original_db_view' not in st.session_state:
        st.session_state.original_db_view = {
            'pcb': {'results': pd.DataFrame(), 'show': False},
            'fw': {'results': pd.DataFrame(), 'show': False},
            'rftx': {'results': pd.DataFrame(), 'show': False},
            'semi': {'results': pd.DataFrame(), 'show': False},
            'func': {'results': pd.DataFrame(), 'show': False},
        }

def main():
    st.set_page_config(layout="wide")
    st.title("리모컨 생산 데이터 분석 툴")
    st.markdown("---")
    st.info("--- 1. 앱 시작 및 모듈 로드 완료 ---") # 👈 이처럼 추가하세요
    initialize_session_state()
    
    # 모듈이 로드되지 않았으면 앱 실행 중지
    if not modules_loaded:
        st.error("❌ 필수 모듈을 로드할 수 없어 앱 실행을 중지합니다.")
        st.info("위의 해결 방법을 따라 폴더 구조를 수정한 후 다시 실행해주세요.")
        st.stop()
    st.info("--- 2. 데이터베이스 연결 시도 ---") # 👈 이처럼 추가하세요
    # 데이터베이스 연결 시도
    try:
        conn = get_connection()
        if conn is None:
            st.error("❌ 데이터베이스 연결에 실패했습니다.")
            st.info("db_utils.py 파일과 데이터베이스 파일을 확인해주세요.")
            st.stop()
    except Exception as e:
        st.error(f"❌ 데이터베이스 연결 중 예외 발생: {e}")
        st.stop()
    
    st.info("--- 3. 데이터베이스 연결 성공 및 테이블 로드 시작 ---") # 👈 이처럼 추가하세요    
    # 데이터베이스 테이블 조회 전 테이블 존재 여부 확인
    try:
        # 테이블 목록 확인
        table_check = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
        st.success(f"✅ 데이터베이스 연결 성공! {len(table_check)}개의 테이블이 있습니다.")
        
        with st.expander("데이터베이스 테이블 목록 보기"):
            st.write("존재하는 테이블들:")
            st.dataframe(table_check)
        
        # historyinspection 테이블이 존재하는지 확인
        available_tables = table_check['name'].str.lower().tolist()
        target_table = None
        
        # 가능한 테이블 이름들 확인
        possible_names = ['historyinspection', 'history_inspection', 'inspection', 'history']
        for name in possible_names:
            if name.lower() in available_tables:
                target_table = table_check[table_check['name'].str.lower() == name.lower()]['name'].iloc[0]
                break
        
        if target_table:
            df_all_data = pd.read_sql_query(f"SELECT * FROM {target_table};", conn)
            st.success(f"✅ '{target_table}' 테이블 로드 완료! (총 {len(df_all_data):,}개 레코드)")
        else:
            st.error("❌ 'historyinspection' 또는 유사한 테이블을 찾을 수 없습니다.")
            st.info("위의 테이블 목록에서 올바른 테이블 이름을 확인하고 코드를 수정해주세요.")
            st.stop()
            
    except Exception as e:
        st.error(f"❌ 데이터베이스 조회 중 오류: {e}")
        st.stop()

    # 날짜 컬럼 변환
    try:
        df_all_data['PcbStartTime_dt'] = pd.to_datetime(df_all_data['PcbStartTime'], errors='coerce')
        df_all_data['FwStamp_dt'] = pd.to_datetime(df_all_data['FwStamp'], errors='coerce')
        df_all_data['RfTxStamp_dt'] = pd.to_datetime(df_all_data['RfTxStamp'], errors='coerce')
        df_all_data['SemiAssyStartTime_dt'] = pd.to_datetime(df_all_data['SemiAssyStartTime'], errors='coerce')
        df_all_data['BatadcStamp_dt'] = pd.to_datetime(df_all_data['BatadcStamp'], errors='coerce')
        st.success("✅ 날짜 컬럼 변환 완료")
    except Exception as e:
        st.error(f"❌ 날짜 컬럼 변환 실패: {e}")
        st.info("데이터베이스의 날짜 컬럼명을 확인해주세요.")
        # 컬럼 정보 표시
        st.write("현재 데이터베이스 컬럼:")
        st.write(list(df_all_data.columns))

    tab_info = {
        'pcb': {'header': "파일 PCB (Pcb_Process)", 'date_col': 'PcbStartTime_dt'},
        'fw': {'header': "파일 Fw (Fw_Process)", 'date_col': 'FwStamp_dt'},
        'rftx': {'header': "파일 RfTx (RfTx_Process)", 'date_col': 'RfTxStamp_dt'},
        'semi': {'header': "파일 Semi (SemiAssy_Process)", 'date_col': 'SemiAssyStartTime_dt'},
        'func': {'header': "파일 Func (Func_Process)", 'date_col': 'BatadcStamp_dt'}
    }

    tabs = st.tabs(list(tab_info.keys()))

    for i, tab_key in enumerate(tab_info.keys()):
        with tabs[i]:
            st.header(tab_info[tab_key]['header'])

            try:
                jig_col_name = st.session_state.jig_col_mapping[tab_key]
                
                # 컬럼 존재 확인
                if jig_col_name not in df_all_data.columns:
                    st.warning(f"⚠️ '{jig_col_name}' 컬럼을 찾을 수 없습니다.")
                    st.info("사용 가능한 컬럼:")
                    st.write([col for col in df_all_data.columns if 'PC' in col or 'Jig' in col])
                    continue
                
                unique_jigs = df_all_data[jig_col_name].dropna().unique()
                pc_options = ['모든 PC'] + sorted(list(unique_jigs))
                selected_jig = st.selectbox("PC (Jig) 선택", pc_options, key=f"pc_select_{tab_key}")

                date_col = tab_info[tab_key]['date_col']
                if date_col not in df_all_data.columns:
                    st.error(f"❌ 날짜 컬럼 '{date_col}'을 찾을 수 없습니다.")
                    continue
                
                df_dates = df_all_data[date_col].dt.date.dropna()
                if not df_dates.empty:
                    min_date = df_dates.min()
                    max_date = df_dates.max()
                else:
                    min_date = max_date = date.today()
                
                selected_dates = st.date_input("날짜 범위 선택", value=(min_date, max_date), key=f"dates_{tab_key}")
                
                if st.button("분석 실행", key=f"analyze_{tab_key}"):
                    with st.spinner("데이터 분석 및 저장 중..."):
                        if len(selected_dates) == 2:
                            start_date, end_date = selected_dates
                            df_filtered = df_all_data[
                                (df_all_data[date_col].dt.date >= start_date) &
                                (df_all_data[date_col].dt.date <= end_date)
                            ].copy()
                            if selected_jig != '모든 PC':
                                df_filtered = df_filtered[df_filtered[jig_col_name] == selected_jig].copy()
                        else:
                            st.warning("날짜 범위를 올바르게 선택해주세요.")
                            df_filtered = pd.DataFrame()
                        
                        st.session_state.analysis_results[tab_key] = df_filtered
                        st.session_state.analysis_data[tab_key] = analyze_data(df_filtered, date_col, jig_col_name)
                        st.session_state.analysis_time[tab_key] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        st.session_state.analysis_status[tab_key]['analyzed'] = True
                    st.success("분석 완료! 결과가 저장되었습니다.")

                if st.session_state.analysis_status[tab_key]['analyzed']:
                    display_analysis_result(tab_key, tab_info[tab_key]['header'], date_col,
                                            selected_jig=selected_jig if selected_jig != '모든 PC' else None,
                                            used_jig_col=st.session_state.analysis_data[tab_key][2])
                
                st.markdown("---")
                st.markdown(f"#### {tab_info[tab_key]['header'].split()[1]} 데이터 조회")
                display_data_views(tab_key, df_all_data)
                
            except Exception as e:
                st.error(f"❌ 탭 '{tab_key}' 처리 중 오류: {e}")
                st.info("이 탭은 건너뛰고 다른 탭을 사용해보세요.")

    st.markdown("---")
    st.markdown("<p style='text-align:center'>Copyright © 2024</p>", unsafe_allow_html=True)
            
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 앱 실행 중 치명적 오류 발생: {e}", file=sys.stderr)
        print("개발자에게 문의하거나 로그를 확인해주세요.", file=sys.stderr)
        st.error(f"❌ 앱 실행 중 치명적 오류 발생: {e}")
        st.info("개발자에게 문의하거나 로그를 확인해주세요.")