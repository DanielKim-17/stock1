import streamlit as st
import FinanceDataReader as fdr
import pandas as pd
import datetime
import yfinance as yf
import re

# Page configuration
st.set_page_config(page_title="Stock Volume Analysis", layout="wide")

st.title("📈 Stock Volume Analysis Application")
st.markdown("""
지정한 주식의 **최근 거래량**이 **이전 20일 평균 거래량의 150%**를 초과하는지 확인합니다.
한국 주식(6자리 코드) 및 해외 주식(티커)을 모두 지원합니다.
""")

# Input for Tickers
default_tickers = "EDV, UBT, VALE, ALB, UNH, 9988.HK, 9888.HK, 9618.HK, 3988.HK, 0883.HK, 1211.HK, 3690.HK, DIS, AES, PFE, 005490"
ticker_input = st.text_area("종목 코드 입력 (콤마 또는 공백으로 구분)", value=default_tickers, height=70)

# Process Tickers
tickers = [t.strip() for t in ticker_input.replace(',', ' ').split() if t.strip()]

# Filter Option
show_only_targets = st.checkbox("조건 만족 종목만 보기 (최근 거래량 > 20일 평균의 150%)", value=True)

if st.button("분석 시작"):
    if not tickers:
        st.warning("종목 코드를 입력해주세요.")
    else:
        st.info("데이터를 분석 중입니다...")
        
        # Get Stock Listing for Names (KRX only)
        @st.cache_data
        def get_krx_listing():
            try:
                return fdr.StockListing('KRX')
            except Exception as e:
                st.warning(f"KRX 종목 목록을 가져오는데 실패했습니다 ({e}). 한국 종목명이 표시되지 않을 수 있습니다.")
                return None
            
        krx = get_krx_listing()
        
        results = []
        progress_bar = st.progress(0)
        
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=60) 
        
        for i, raw_code in enumerate(tickers):
            try:
                # 1. Identify and Clean Code
                # Remove .KR suffix if 6 digits precede code (common user input issue)
                code = raw_code
                if code.endswith('.KR') and len(code) == 9 and code[:6].isdigit():
                     code = code[:6]
                
                is_kr_stock = code.isdigit() and len(code) == 6
                
                # 2. Get Name
                name = "Unknown"
                if is_kr_stock:
                    if krx is not None:
                        name_row = krx[krx['Code'] == code]
                        name = name_row['Name'].values[0] if not name_row.empty else "Unknown"
                else:
                    # Foreign stock: Try yfinance for name
                    try:
                        t = yf.Ticker(code)
                        # Accessing info might be slow, consider caching if reused often, 
                        # but for a list of ~20 it's usually okay.
                        # Sometimes shortName is missing, try longName or symbol.
                        name = t.info.get('shortName', t.info.get('longName', code))
                    except:
                        name = code

                # 3. Fetch Data
                # KR stocks: fdr.DataReader(code) works (uses KRX/Naver)
                # Foreign: fdr.DataReader(code) works (uses Yahoo)
                df = fdr.DataReader(code, start_date)
                
                if len(df) < 21:
                    st.warning(f"{code} ({name}): 데이터가 부족합니다 ({len(df)}일). 건너뜁니다.")
                    continue
                
                curr_data = df.iloc[-1]
                prev_20_data = df.iloc[-21:-1]
                prev_3_data = df.iloc[-4:-1]
                
                curr_vol = curr_data['Volume']
                curr_price = curr_data['Close']
                
                # Price Change Calculation
                if 'Change' in df.columns:
                    price_change = curr_data['Change'] * 100 
                else:
                    prev_close = df.iloc[-2]['Close']
                    price_change = ((curr_price - prev_close) / prev_close) * 100
                    
                avg_vol_20 = prev_20_data['Volume'].mean()
                avg_vol_3 = prev_3_data['Volume'].mean()
                
                ratio_20 = (curr_vol / avg_vol_20) * 100 if avg_vol_20 > 0 else 0
                ratio_3 = (curr_vol / avg_vol_3) * 100 if avg_vol_3 > 0 else 0
                
                is_target = ratio_20 > 150
                
                results.append({
                    'Ticker': code,
                    'Name': name,
                    '현재주가': f"{curr_price:,.2f}" if not is_kr_stock else f"{curr_price:,.0f}", # Floating point for US/HK
                    '상승률': f"{price_change:+.2f}%",
                    '최근 1일 거래량': f"{curr_vol:,.0f}",
                    '3일 평균 거래량': f"{avg_vol_3:,.0f}",
                    '20일 평균 거래량': f"{avg_vol_20:,.0f}",
                    '3일비 거래량 비율': f"{ratio_3:.1f}%",
                    '20일비 거래량 비율': f"{ratio_20:.1f}%",
                    'Condition': is_target,
                    'Raw_Ratio_20': ratio_20 
                })
                
            except Exception as e:
                st.error(f"Error processing {raw_code}: {e}")
            
            progress_bar.progress((i + 1) / len(tickers))
            
        progress_bar.empty()
        
        if results:
            res_df = pd.DataFrame(results)
            
            if show_only_targets:
                display_df = res_df[res_df['Condition'] == True].copy()
            else:
                display_df = res_df.copy()
            
            # Drop helper columns
            final_df = display_df.drop(columns=['Condition', 'Raw_Ratio_20'])
            
            st.success(f"분석 완료! 총 {len(res_df)}개 중 {len(display_df)}개 종목이 표시됩니다.")
            st.dataframe(final_df, use_container_width=True)
        else:
            st.warning("결과가 없습니다.")
