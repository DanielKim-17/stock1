import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import pickle
from datetime import datetime, timedelta
import numpy as np
import concurrent.futures
import json

# --- Configuration ---
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
GOOGLE_DRIVE_PATH = './'
DAILY_DATA_FILE = os.path.join(GOOGLE_DRIVE_PATH, 'sp500_daily.pkl')
STOCK_INFO_FILE = os.path.join(GOOGLE_DRIVE_PATH, 'stockinfo.pkl')

st.set_page_config(layout="wide", page_title="Rising Stock Analysis")

# --- Helper Functions (Cached) ---

@st.cache_data(ttl=3600)
def load_tickers_from_sheet(spreadsheet_name='stock_list', sheet_name='시트1', col_idx=1):
    creds = None
    # 1. Try Local File
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
        except Exception as e:
            st.error(f"Local key error: {e}")
            return []
    # 2. Try Streamlit Secrets (Cloud)
    elif 'gcp_service_account' in st.secrets:
        try:
            # st.secrets returns a plain dict for the nested section
            creds_dict = dict(st.secrets['gcp_service_account'])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
        except Exception as e:
            st.error(f"Secrets key error: {e}")
            return []
            
    # 3. Try Streamlit Secrets (JSON String) - Easier for users
    elif 'gcp_json' in st.secrets:
        try:
            creds_dict = json.loads(st.secrets['gcp_json'], strict=False)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
        except Exception as e:
            st.error(f"Secrets JSON error: {e}")
            return []

    if not creds:
        st.error("Authentication credentials not found (JSON or Secrets).")
        return []

    try:
        client = gspread.authorize(creds)
        all_sheets = client.openall()
        doc = None
        for sh in all_sheets:
            if sh.title == spreadsheet_name:
                doc = sh
                break
        if not doc:
            for sh in all_sheets:
                if spreadsheet_name in sh.title:
                    doc = sh
                    break
        if not doc: return []
        try: sheet = doc.worksheet(sheet_name)
        except: sheet = doc.get_worksheet(0)
        values = sheet.col_values(col_idx)
        if not values: return []
        if values and values[0].lower() == 'ticker': values = values[1:]
        return [v.strip().upper() for v in values if v.strip()]
    except Exception as e:
        st.error(f"Google Sheet Error: {e}")
        return []

def update_market_data(tickers):
    market_data = pd.DataFrame()
    if os.path.exists(DAILY_DATA_FILE):
        try:
            with open(DAILY_DATA_FILE, 'rb') as f: market_data = pickle.load(f)
        except: pass

    today = datetime.now().date()
    if market_data.empty:
        new_tickers = tickers
        start_date_for_update = None
        update_tickers = []
    else:
        try:
            existing_tickers = market_data.columns.get_level_values(1).unique().tolist()
            last_date = market_data.index[-1].date()
            new_tickers = list(set(tickers) - set(existing_tickers))
            update_tickers = list(set(tickers) & set(existing_tickers))
            start_date_for_update = last_date + timedelta(days=1) if last_date < today else None
        except:
            new_tickers = tickers
            update_tickers = []
            market_data = pd.DataFrame()
            start_date_for_update = None

    new_df, update_df = pd.DataFrame(), pd.DataFrame()
    if new_tickers:
        with st.spinner(f"Downloading price history for {len(new_tickers)} new tickers..."):
             new_df = yf.download(new_tickers, period='1y', threads=True, auto_adjust=True)
    if update_tickers and start_date_for_update and start_date_for_update <= today:
        with st.spinner(f"Updating price history from {start_date_for_update}..."):
             update_df = yf.download(update_tickers, start=start_date_for_update, threads=True, auto_adjust=True)
    
    final_df = market_data
    if market_data.empty: final_df = new_df
    else:
        if not update_df.empty:
             final_df = pd.concat([final_df, update_df])
             final_df = final_df[~final_df.index.duplicated(keep='last')]
        if not new_df.empty:
             final_df = final_df.join(new_df, how='outer')
             
    if not final_df.empty:
        with open(DAILY_DATA_FILE, 'wb') as f: pickle.dump(final_df, f)
    return final_df

def analyze_stage1(market_data, tickers):
    if market_data.empty: return pd.DataFrame()
    try:
        close = market_data['Close']
        high = market_data['High']
        low = market_data['Low']
        volume = market_data['Volume']
    except: return pd.DataFrame()

    candidates = []
    valid_tickers = [t for t in tickers if t in close.columns]
    
    for symbol in valid_tickers:
        try:
            c_series = close[symbol].dropna()
            if len(c_series) < 60: continue
            
            h60 = high[symbol].dropna().tail(60).max()
            l60 = low[symbol].dropna().tail(60).min()
            volatility = (h60 - l60) / h60
            curr_price = c_series.iloc[-1]
            is_vcp = volatility < 0.20 and (curr_price > h60 * 0.85)
            
            v_series = volume[symbol].dropna()
            curr_vol = v_series.iloc[-1]
            vol_20_avg = v_series.tail(20).mean()
            is_vol_spike = curr_vol > (vol_20_avg * 1.03)
            
            if is_vcp:
                candidates.append({'Ticker': symbol,'VCP': True,'Vol_Spike': is_vol_spike,'Price': curr_price})
        except: continue
    return pd.DataFrame(candidates)

def get_stock_info_data(tickers):
    """
    Manages stockinfo.pkl.
    Fields: Name, Sector, Inst_Own, Turnaround, NewsList (List of dicts), Metrics...
    """
    cached_df = pd.DataFrame()
    if os.path.exists(STOCK_INFO_FILE):
        try:
            with open(STOCK_INFO_FILE, 'rb') as f:
                cached_df = pickle.load(f)
        except: pass
    
    current_time = datetime.now()
    one_week_ago = current_time - timedelta(days=7)
    
    # Init Cache Columns if new schema
    if not cached_df.empty and 'LastUpdated' not in cached_df.columns:
        cached_df['LastUpdated'] = datetime.now() - timedelta(days=365)
    
    tickers_to_update = []
    
    if cached_df.empty:
        tickers_to_update = tickers
        cached_df = pd.DataFrame(columns=['Ticker', 'LastUpdated'])
    else:
        existing_tickers = cached_df['Ticker'].unique().tolist()
        missing = list(set(tickers) - set(existing_tickers))
        
        # Check expired
        if 'LastUpdated' in cached_df.columns:
             cached_df['LastUpdated'] = pd.to_datetime(cached_df['LastUpdated'])
             expired_rows = cached_df[cached_df['LastUpdated'] < one_week_ago]
             expired = expired_rows['Ticker'].tolist()
        else:
             expired = []

        # Check for missing Schema Columns (e.g. Name, News_List)
        missing_schema = []
        if 'Name' not in cached_df.columns or 'RecMean' not in cached_df.columns:
             missing_schema = existing_tickers # Update all if schema changed

        tickers_to_update = list(set(missing + expired + missing_schema))
        tickers_to_update = [t for t in tickers_to_update if t in tickers]

    if tickers_to_update:
        status_text = st.empty()
        status_text.write(f"Updating fundamental data for {len(tickers_to_update)} tickers (Expired or Schema Update)...")
        
        results = []
        def fetch_one(symbol):
            try:
                stock = yf.Ticker(symbol)
                info = stock.info
                
                # Turnaround
                financials = stock.quarterly_financials
                turnaround_status = "N/A"
                if not financials.empty and 'Net Income' in financials.index:
                    ni = financials.loc['Net Income']
                    if len(ni) >= 2:
                        rec, prev = ni.iloc[0], ni.iloc[1]
                        if rec > 0 and prev > 0: turnaround_status = "Profit Growth" if rec > prev else "Profit (Declining)"
                        elif rec > 0 >= prev: turnaround_status = "Turn to Black"
                        elif rec <= 0 and prev <= 0: turnaround_status = "Deficit Reduction" if rec > prev else "Deficit (Worsening)"
                        else: turnaround_status = "Turn to Red"
                
                # News
                news_list = stock.news
                news_items = []
                pos_keys = ['launch', 'growth', 'approve', 'contract', 'partnership', 'record']
                
                if news_list:
                    for n in news_list[:5]:
                        pub = n.get('providerPublishTime')
                        if pub and (current_time - datetime.fromtimestamp(pub)).total_seconds() > 48*3600: continue
                        title = n.get('content', {}).get('title', n.get('title', ''))
                        link = n.get('content', {}).get('clickThroughUrl', {}).get('url', n.get('link', ''))
                        
                        is_good = any(k in title.lower() for k in pos_keys)
                        news_items.append({'title': title, 'link': link, 'good': is_good})
                
                has_good_news = any(n['good'] for n in news_items)

                return {
                    'Ticker': symbol,
                    'Name': info.get('shortName', symbol),
                    'Sector': info.get('sector', 'Unknown'),
                    'Inst_Own': info.get('heldPercentInstitutions', 0),
                    'Turnaround': turnaround_status,
                    'Good_News': has_good_news,
                    'News_List': news_items, 
                    'PER': info.get('trailingPE'),
                    'PBR': info.get('priceToBook'),
                    'PSR': info.get('priceToSalesTrailing12Months'),
                    'EV/EBITDA': info.get('enterpriseToEbitda'),
                    'RevGrowth': info.get('revenueGrowth'),
                    'EPSGrowth': info.get('earningsGrowth'),
                    'DivYield': info.get('dividendYield'),
                    'RecKey': info.get('recommendationKey'),
                    'RecMean': info.get('recommendationMean'),
                    'LastUpdated': current_time
                }
            except: return None
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for f in concurrent.futures.as_completed([executor.submit(fetch_one, t) for t in tickers_to_update]):
                res = f.result()
                if res: results.append(res)
        
        if results:
            new_data_df = pd.DataFrame(results)
            # Remove old rows for updated tickers
            cached_df = cached_df[~cached_df['Ticker'].isin(new_data_df['Ticker'])]
            # Concat
            cached_df = pd.concat([cached_df, new_data_df], ignore_index=True)
            
            with open(STOCK_INFO_FILE, 'wb') as f:
                pickle.dump(cached_df, f)
        
        status_text.empty()
        
    return cached_df


# --- UI Layout ---

st.title("📈 Rising Stock Analysis")

with st.expander("Configuration & Data Update", expanded=False):
    col1, col2 = st.columns([3, 1])
    with col1:
       sheet_name_input = st.text_input("Google Sheet Name", "stock_list")
    with col2:
        if st.button("Load & Update Data"):
            with st.spinner("Processing..."):
                tickers = load_tickers_from_sheet(sheet_name_input)
                if tickers:
                    st.session_state['tickers'] = tickers
                    mk_data = update_market_data(tickers)
                    st.session_state['market_data'] = mk_data
                    st.success(f"Market Data Updated ({len(tickers)}).")
                    if 'selected_tickers' in st.session_state: del st.session_state['selected_tickers']

if 'market_data' not in st.session_state and os.path.exists(DAILY_DATA_FILE):
    try:
        with open(DAILY_DATA_FILE, 'rb') as f:
            st.session_state['market_data'] = pickle.load(f)
            st.session_state['tickers'] = st.session_state['market_data'].columns.get_level_values(1).unique().tolist()
    except: pass

if 'market_data' in st.session_state and not st.session_state['market_data'].empty:
    market_data = st.session_state['market_data']
    tickers = st.session_state.get('tickers', [])
    
    stage1_df = analyze_stage1(market_data, tickers)
    
    if not stage1_df.empty:
        candidate_tickers = stage1_df['Ticker'].tolist()
        info_df = get_stock_info_data(candidate_tickers)
        
        if not info_df.empty:
            final_df = pd.merge(stage1_df, info_df, on='Ticker', how='inner')
        else:
            final_df = stage1_df
            for col in ['Name', 'Sector','Inst_Own','Turnaround','Good_News','PER','PBR','PSR','EV/EBITDA','RevGrowth','EPSGrowth','DivYield','RecKey','RecMean', 'News_List']:
                final_df[col] = None
        
        # Fallback for Name if merger missed it (partial update scenario)
        if 'Name' not in final_df.columns: final_df['Name'] = final_df['Ticker']

        final_df['Inst_Support'] = (final_df['Inst_Own'] > 0.4) & final_df['Vol_Spike']
        
        # --- Filters ---
        st.subheader("Filter Candidates")
        
        def get_range(col):
             if col not in final_df.columns: return 0.0, 100.0
             vals = final_df[col].dropna()
             if vals.empty: return 0.0, 100.0
             return float(vals.min()), float(vals.max())

        min_pe, max_pe = get_range('PER')
        min_pbr, max_pbr = get_range('PBR')
        min_rg, max_rg = get_range('RevGrowth')
        min_eg, max_eg = get_range('EPSGrowth')

        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1: f_pe = st.slider("PER Range", min_value=0.0, max_value=max(100.0, max_pe), value=(0.0, max(100.0, max_pe)))
        with fc2: f_pbr = st.slider("PBR Range", min_value=0.0, max_value=max(20.0, max_pbr), value=(0.0, max(20.0, max_pbr)))
        with fc3: f_rg = st.slider("Rev Growth", min_value=min(-1.0, min_rg), max_value=max(2.0, max_rg), value=(min(-1.0, min_rg), max(2.0, max_rg)))
        with fc4: f_eg = st.slider("EPS Growth", min_value=min(-1.0, min_eg), max_value=max(5.0, max_eg), value=(min(-1.0, min_eg), max(5.0, max_eg)))
        
        c1, c2, c3 = st.columns(3)
        with c1: selected_sectors = st.multiselect("Sector", options=sorted(final_df['Sector'].astype(str).unique()), default=[])
        with c2: selected_turnaround = st.multiselect("Turnaround", options=sorted(final_df['Turnaround'].astype(str).unique()), default=[])
        with c3: support_only = st.checkbox("Inst. Support Only", value=False)
        
        view = final_df.copy()
        if selected_sectors: view = view[view['Sector'].isin(selected_sectors)]
        if selected_turnaround: view = view[view['Turnaround'].isin(selected_turnaround)]
        if support_only: view = view[view['Inst_Support']]
        
        # Numeric Filter
        if 'PER' in view.columns:
            view = view[
                (view['PER'].fillna(0) >= f_pe[0]) & (view['PER'].fillna(0) <= f_pe[1]) &
                (view['PBR'].fillna(0) >= f_pbr[0]) & (view['PBR'].fillna(0) <= f_pbr[1]) &
                (view['RevGrowth'].fillna(-999) >= f_rg[0]) & (view['RevGrowth'].fillna(-999) <= f_rg[1]) &
                (view['EPSGrowth'].fillna(-999) >= f_eg[0]) & (view['EPSGrowth'].fillna(-999) <= f_eg[1])
            ]

        # --- Table ---
        st.markdown(f"**Candidates Found:** {len(view)}")
        if 'Select' not in view.columns: view.insert(0, "Select", False)
        
        display_cols = ['Select', 'Ticker', 'Name', 'Price', 'Sector', 'Inst_Support', 'Turnaround', 'Good_News', 
                        'PER', 'PBR', 'RevGrowth', 'EPSGrowth', 'RecMean']
        
        # Ensure cols exist
        for c in display_cols:
            if c not in view.columns: view[c] = None
        
        edited_df = st.data_editor(
            view[display_cols].style.format({
                "Price": "${:.2f}",
                "PER": "{:.1f}", "PBR": "{:.1f}",
                "RevGrowth": "{:.1%}", "EPSGrowth": "{:.1%}",
                "RecMean": "{:.2f}"
            }),
            column_config={
                "Select": st.column_config.CheckboxColumn("Check", default=False),
                "Inst_Support": st.column_config.CheckboxColumn("Inst"),
                "Good_News": st.column_config.CheckboxColumn("News"),
            },
            disabled=[c for c in display_cols if c != "Select"],
            hide_index=True,
            use_container_width=True,
            height=400
        )
        
        selected_rows = edited_df[edited_df['Select']]
        checked_tickers = selected_rows['Ticker'].tolist()
        
        # --- Detail Analysis ---
        st.divider()
        if checked_tickers:
            st.subheader("Detail Analysis")
            
            period_map = {'1M': 20, '3M': 60, '6M': 120, '1Y': 252}
            pl = st.radio("Period", list(period_map.keys()), index=1, horizontal=True)
            days = period_map[pl]
            close_data = market_data['Close'][checked_tickers].tail(days).copy()
            norm_data = (close_data / close_data.iloc[0] - 1) * 100
            fig = px.line(norm_data, labels={"value": "Return (%)", "variable": "Ticker"})
            st.plotly_chart(fig, use_container_width=True)
            
            m_view = final_df[final_df['Ticker'].isin(checked_tickers)].set_index('Ticker')
            m_view['DivYield'] = m_view['DivYield'] / 100
            
            format_map = {
                'Price': lambda x: f"${x:.2f}",
                'Inst_Own': lambda x: f"{x*100:.1f}%",
                'RevGrowth': lambda x: f"{x*100:.1f}%",
                'EPSGrowth': lambda x: f"{x*100:.1f}%",
                'DivYield': lambda x: f"{x*100:.2f}%" if pd.notnull(x) else "-",
                'PER': lambda x: f"{x:.1f}", 'PBR': lambda x: f"{x:.1f}", 
                'PSR': lambda x: f"{x:.1f}", 'EV/EBITDA': lambda x: f"{x:.1f}",
                'RecMean': lambda x: f"{x:.2f}"
            }
            cols_to_show = ['Name', 'Price','Sector','Inst_Own','PER','PBR','PSR','EV/EBITDA','RevGrowth','EPSGrowth','DivYield','RecKey','RecMean','Turnaround']
            
            # Ensure exist
            for c in cols_to_show:
                if c not in m_view.columns: m_view[c] = None

            for c in cols_to_show:
                if c in format_map:
                    m_view[c] = m_view[c].apply(lambda x: format_map[c](x) if pd.notnull(x) and isinstance(x, (int, float)) else x)
            
            st.markdown("##### Key Metrics")
            st.table(m_view[cols_to_show].T)
            
            st.markdown("##### Recent News")
            cols = st.columns(len(checked_tickers))
            for i, tick in enumerate(checked_tickers):
                with cols[i]:
                    st.markdown(f"**{tick} News**")
                    row = final_df[final_df['Ticker'] == tick].iloc[0]
                    news_list = row.get('News_List')
                    if news_list and isinstance(news_list, list):
                        for news in news_list:
                             title = news.get('title', 'No Title')
                             link = news.get('link', '#')
                             st.markdown(f"- [{title}]({link})")
                    else:
                        st.write("No recent news found.")
        else:
            st.info("Select tickers to view details.")
else:
    st.info("Load data to start.")
