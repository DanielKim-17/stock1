import streamlit as st
import pandas as pd
import datetime
import yfinance as yf
import re

# Page configuration
st.set_page_config(page_title="Stock Volume Analysis", layout="wide")

st.title("📈 Stock Volume Analysis Application")
st.markdown("""
지정한 주식의 **최근 거래량**이 **이전 20일 평균 거래량의 150%**를 초과하는지 확인합니다.
한국 주식(6자리 코드) 및 해외 주식(티커)을 모두 지원합니다. (Powered by yfinance)
""")

# Input for Tickers
default_tickers = "EDV, UBT, VALE, ALB, UNH, 9988.HK, 9888.HK, 9618.HK, 3988.HK, 0883.HK, 1211.HK, 3690.HK, DIS, AES, PFE, 005490"
ticker_input = st.text_area("종목 코드 입력 (콤마 또는 공백으로 구분)", value=default_tickers, height=70)

# Process Tickers
tickers = [t.strip() for t in ticker_input.replace(',', ' ').split() if t.strip()]

# Filter Option
col1, col2 = st.columns(2)
with col1:
    show_only_targets = st.checkbox("조건 만족 종목만 보기 (최근 거래량 > 20일 평균의 150%)", value=True)
with col2:
    use_d_minus_1 = st.checkbox("하루 전 데이터(D-1) 기준으로 분석 (데이터 부족 시 사용)", value=False)

if st.button("분석 시작"):
    if not tickers:
        st.warning("종목 코드를 입력해주세요.")
    else:
        st.info("데이터를 분석 중입니다...")
        
        results = []
        progress_bar = st.progress(0)
        
        today = datetime.date.today()
        # Fetch enough data to calculate 20-day moving average + potential shift
        # 120 days buffer to be safe
        start_date = today - datetime.timedelta(days=120) 
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        for i, raw_code in enumerate(tickers):
            try:
                # 1. Identify and Clean Code
                # Clean up input (remove .KR if present, though we will handle suffixes below)
                code = raw_code.strip()
                if code.endswith('.KR') and len(code) == 9 and code[:6].isdigit():
                     code = code[:6] # Convert 005930.KR -> 005930 for processing
                
                # 2. Determine Symbol to Fetch
                # Logic: 
                # - If pure 6-digit (e.g. 005930), try .KS first. If empty, try .KQ.
                # - If already has .KS/.KQ, use as is.
                # - If foreign, use as is.
                
                is_pure_kr_digit = (code.isdigit() and len(code) == 6)
                symbols_to_try = []
                
                if is_pure_kr_digit:
                    symbols_to_try = [f"{code}.KS", f"{code}.KQ"]
                else:
                    symbols_to_try = [code]
                
                df = pd.DataFrame()
                final_symbol = code
                ticker_obj = None
                
                for sym in symbols_to_try:
                    t = yf.Ticker(sym)
                    # fetch history
                    hist = t.history(start=start_date_str, auto_adjust=False)
                    
                    if not hist.empty:
                        df = hist
                        final_symbol = sym
                        ticker_obj = t
                        break
                
                # Check Data
                # Determine Minimum Rows needed
                min_rows = 22 if use_d_minus_1 else 21
                
                if df.empty or len(df) < min_rows:
                    st.warning(f"{code}: 데이터가 부족하거나 찾을 수 없습니다 ({len(df)}일 / 필요: {min_rows}일). 건너뜁니다.")
                    continue
                
                # 3. Get Name (from yfinance info)
                name = code
                try:
                    info = ticker_obj.info
                    # Prefer shortName, then longName
                    name = info.get('shortName', info.get('longName', code))
                except:
                    pass

                # 4. Logic Slicing
                # yfinance history index is DatetimeIndex usually timezone-aware
                
                if use_d_minus_1:
                    curr_data = df.iloc[-2]
                    prev_20_data = df.iloc[-22:-2]
                    prev_3_data = df.iloc[-5:-2]
                else:
                    curr_data = df.iloc[-1]
                    prev_20_data = df.iloc[-21:-1]
                    prev_3_data = df.iloc[-4:-1]
                
                curr_vol = curr_data['Volume']
                curr_price = curr_data['Close']
                
                # Handle timezone aware timestamps for display
                if hasattr(curr_data.name, 'date'):
                     curr_date = curr_data.name.date()
                else:
                     curr_date = str(curr_data.name).split()[0]
                
                # Price Change Calculation
                # yfinance doesn't explicitly give 'Change' in history, need to calculate
                # D-0: prev is -2, D-1: prev is -3
                prev_idx_offset = -3 if use_d_minus_1 else -2
                prev_close = df.iloc[prev_idx_offset]['Close']
                
                if prev_close > 0:
                    price_change = ((curr_price - prev_close) / prev_close) * 100
                else:
                    price_change = 0.0
                    
                avg_vol_20 = prev_20_data['Volume'].mean()
                avg_vol_3 = prev_3_data['Volume'].mean()
                
                ratio_20 = (curr_vol / avg_vol_20) * 100 if avg_vol_20 > 0 else 0
                ratio_3 = (curr_vol / avg_vol_3) * 100 if avg_vol_3 > 0 else 0
                
                is_target = ratio_20 > 150
                
                # Formatting: Integer for KR, Float for others
                # Heuristic: If symbol ends with .KS or .KQ, treat as KR (Integer)
                is_kr_format = final_symbol.endswith('.KS') or final_symbol.endswith('.KQ')
                
                results.append({
                    'Ticker': final_symbol, # Show the resolved symbol (e.g. 005490.KS)
                    'Name': name,
                    'Date': curr_date,
                    '현재주가': f"{curr_price:,.0f}" if is_kr_format else f"{curr_price:,.2f}", 
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
            st.dataframe(final_df)
        else:
            st.warning("결과가 없습니다.")
