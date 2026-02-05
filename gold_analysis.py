"""
Gold Safe Haven Analysis
========================
Produces a comprehensive analysis spreadsheet examining:
1. Annual correlations between Gold and S&P 500 returns
2. Annual volatility (std dev of daily log returns) for Gold and S&P 500
3. Annual cumulative returns
4. Crisis period analysis
5. Discussion and key findings

Data source: "Data and Charts.xlsx" - Overall sheet
Period: 1971 (end of Bretton Woods) to 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# 1. LOAD DATA
# =============================================================================
print("Loading data...")
wb_path = '/home/user/Gold_safe_haven/Data and Charts.xlsx'

# Read the Overall sheet - prices in columns B:D, returns in columns G:I
df_prices = pd.read_excel(wb_path, sheet_name='Overall', usecols='B:D',
                          skiprows=0, header=0,
                          names=['Date_Price', 'Gold_Price', 'SP500_Price'])

df_returns = pd.read_excel(wb_path, sheet_name='Overall', usecols='G:I',
                           skiprows=0, header=0,
                           names=['Date_Return', 'Gold_Return', 'SP500_Return'])

# Clean up - drop rows where date is NaT
df_prices = df_prices.dropna(subset=['Date_Price'])
df_returns = df_returns.dropna(subset=['Date_Return'])

# Ensure proper types
df_prices['Date_Price'] = pd.to_datetime(df_prices['Date_Price'])
df_returns['Date_Return'] = pd.to_datetime(df_returns['Date_Return'])
df_prices['Gold_Price'] = pd.to_numeric(df_prices['Gold_Price'], errors='coerce')
df_prices['SP500_Price'] = pd.to_numeric(df_prices['SP500_Price'], errors='coerce')
df_returns['Gold_Return'] = pd.to_numeric(df_returns['Gold_Return'], errors='coerce')
df_returns['SP500_Return'] = pd.to_numeric(df_returns['SP500_Return'], errors='coerce')

# Sort by date
df_prices = df_prices.sort_values('Date_Price').reset_index(drop=True)
df_returns = df_returns.sort_values('Date_Return').reset_index(drop=True)

# Add year column
df_returns['Year'] = df_returns['Date_Return'].dt.year
df_prices['Year'] = df_prices['Date_Price'].dt.year

print(f"Price data: {df_prices['Date_Price'].min()} to {df_prices['Date_Price'].max()}")
print(f"Return data: {df_returns['Date_Return'].min()} to {df_returns['Date_Return'].max()}")
print(f"Total return observations: {len(df_returns)}")

# =============================================================================
# 2. COMPUTE ANNUAL STATISTICS
# =============================================================================
print("Computing annual statistics...")

# Group by year
years = sorted(df_returns['Year'].unique())

annual_stats = []
for year in years:
    yr_data = df_returns[df_returns['Year'] == year].copy()
    yr_prices = df_prices[df_prices['Year'] == year].copy()

    if len(yr_data) < 10:  # need minimum observations
        continue

    n_obs = len(yr_data)

    # Daily log returns are already computed - use them directly
    gold_rets = yr_data['Gold_Return'].dropna()
    sp_rets = yr_data['SP500_Return'].dropna()

    # Annual volatility = std dev of daily log returns * sqrt(252) for annualized
    gold_daily_vol = gold_rets.std()
    sp_daily_vol = sp_rets.std()
    gold_annual_vol = gold_daily_vol * np.sqrt(252)
    sp_annual_vol = sp_daily_vol * np.sqrt(252)

    # Cumulative annual return = sum of daily log returns (property of log returns)
    gold_annual_return = gold_rets.sum()
    sp_annual_return = sp_rets.sum()

    # Correlation between gold and SP500 daily returns within the year
    if len(gold_rets) > 2 and len(sp_rets) > 2:
        # Align the series
        merged = yr_data[['Gold_Return', 'SP500_Return']].dropna()
        if len(merged) > 2:
            corr = merged['Gold_Return'].corr(merged['SP500_Return'])
        else:
            corr = np.nan
    else:
        corr = np.nan

    # Year-end and year-start prices for context
    if len(yr_prices) > 0:
        gold_start = yr_prices['Gold_Price'].iloc[0]
        gold_end = yr_prices['Gold_Price'].iloc[-1]
        sp_start = yr_prices['SP500_Price'].iloc[0]
        sp_end = yr_prices['SP500_Price'].iloc[-1]
    else:
        gold_start = gold_end = sp_start = sp_end = np.nan

    # Volatility ratio: Gold vol / SP500 vol
    vol_ratio = gold_annual_vol / sp_annual_vol if sp_annual_vol != 0 else np.nan

    # Max drawdown within the year (using daily log returns)
    gold_cumret = gold_rets.cumsum()
    gold_running_max = gold_cumret.cummax()
    gold_drawdown = (gold_cumret - gold_running_max).min()

    sp_cumret = sp_rets.cumsum()
    sp_running_max = sp_cumret.cummax()
    sp_drawdown = (sp_cumret - sp_running_max).min()

    annual_stats.append({
        'Year': year,
        'N_Obs': n_obs,
        'Gold_Annual_Return': gold_annual_return,
        'SP500_Annual_Return': sp_annual_return,
        'Gold_Daily_Vol': gold_daily_vol,
        'SP500_Daily_Vol': sp_daily_vol,
        'Gold_Annualised_Vol': gold_annual_vol,
        'SP500_Annualised_Vol': sp_annual_vol,
        'Correlation': corr,
        'Vol_Ratio_Gold_SP': vol_ratio,
        'Gold_Max_Drawdown': gold_drawdown,
        'SP500_Max_Drawdown': sp_drawdown,
        'Gold_Start_Price': gold_start,
        'Gold_End_Price': gold_end,
        'SP500_Start_Price': sp_start,
        'SP500_End_Price': sp_end,
    })

df_annual = pd.DataFrame(annual_stats)
print(f"Annual statistics computed for {len(df_annual)} years")

# =============================================================================
# 3. IDENTIFY KEY PERIODS AND REGIMES
# =============================================================================
print("Identifying regimes and crisis periods...")

# Define key periods for context
crisis_periods = [
    {'Period': 'Oil Embargo / Post-Bretton Woods', 'Start': 1973, 'End': 1974,
     'Description': 'OPEC oil embargo, end of gold standard transition, stagflation'},
    {'Period': 'Second Oil Shock / Volcker Tightening', 'Start': 1979, 'End': 1980,
     'Description': 'Iranian revolution, gold spikes to $850, Volcker rate hikes'},
    {'Period': 'Black Monday', 'Start': 1987, 'End': 1987,
     'Description': 'Stock market crash of October 1987, S&P 500 fell ~20% in one day'},
    {'Period': 'Early 1990s Recession', 'Start': 1990, 'End': 1991,
     'Description': 'Gulf War, S&L crisis, US recession'},
    {'Period': 'Dot-com Crash', 'Start': 2000, 'End': 2002,
     'Description': 'Tech bubble burst, 9/11 attacks, corporate scandals'},
    {'Period': 'Global Financial Crisis', 'Start': 2007, 'End': 2009,
     'Description': 'Subprime mortgage crisis, Lehman Brothers collapse, global recession'},
    {'Period': 'European Debt Crisis', 'Start': 2010, 'End': 2012,
     'Description': 'Greek debt crisis, eurozone contagion, gold peaks at $1,895'},
    {'Period': 'Gold Bear Market', 'Start': 2013, 'End': 2015,
     'Description': 'Gold drops ~40% from 2011 peak, taper tantrum, strong USD'},
    {'Period': 'COVID-19 Pandemic', 'Start': 2020, 'End': 2020,
     'Description': 'Global pandemic, markets crashed March 2020, gold fell alongside stocks'},
    {'Period': 'Post-COVID Inflation / Rate Hikes', 'Start': 2022, 'End': 2023,
     'Description': 'High inflation, aggressive Fed tightening, bank failures'},
    {'Period': 'Gold Surge', 'Start': 2024, 'End': 2025,
     'Description': 'Gold surges past $2,700 amid geopolitical tensions, central bank buying'},
]

# Crisis period statistics
crisis_stats = []
for cp in crisis_periods:
    mask = (df_annual['Year'] >= cp['Start']) & (df_annual['Year'] <= cp['End'])
    subset = df_annual[mask]
    if len(subset) > 0:
        avg_corr = subset['Correlation'].mean()
        avg_gold_vol = subset['Gold_Annualised_Vol'].mean()
        avg_sp_vol = subset['SP500_Annualised_Vol'].mean()
        avg_vol_ratio = subset['Vol_Ratio_Gold_SP'].mean()
        total_gold_ret = subset['Gold_Annual_Return'].sum()
        total_sp_ret = subset['SP500_Annual_Return'].sum()
        crisis_stats.append({
            'Period': cp['Period'],
            'Years': f"{cp['Start']}-{cp['End']}",
            'Description': cp['Description'],
            'Avg_Correlation': avg_corr,
            'Gold_Cumulative_Return': total_gold_ret,
            'SP500_Cumulative_Return': total_sp_ret,
            'Gold_Avg_Annual_Vol': avg_gold_vol,
            'SP500_Avg_Annual_Vol': avg_sp_vol,
            'Avg_Vol_Ratio': avg_vol_ratio,
        })

df_crisis = pd.DataFrame(crisis_stats)

# =============================================================================
# 4. REGIME ANALYSIS (Pre-2005 vs Post-2005, matching the article)
# =============================================================================
print("Computing regime statistics...")

# Also: Pre-2005 (stable gold era) vs Post-2005 (volatile gold era)
pre2005 = df_annual[df_annual['Year'] <= 2005]
post2005 = df_annual[df_annual['Year'] > 2005]

# Full sample
full = df_annual.copy()

regime_summary = []
for label, subset in [('Full Sample (1971-2025)', full),
                       ('Stable Era (1971-2005)', pre2005),
                       ('Volatile Era (2006-2025)', post2005)]:
    regime_summary.append({
        'Regime': label,
        'N_Years': len(subset),
        'Avg_Gold_Return': subset['Gold_Annual_Return'].mean(),
        'Avg_SP500_Return': subset['SP500_Annual_Return'].mean(),
        'Median_Gold_Return': subset['Gold_Annual_Return'].median(),
        'Median_SP500_Return': subset['SP500_Annual_Return'].median(),
        'Avg_Gold_Annualised_Vol': subset['Gold_Annualised_Vol'].mean(),
        'Avg_SP500_Annualised_Vol': subset['SP500_Annualised_Vol'].mean(),
        'Median_Gold_Annualised_Vol': subset['Gold_Annualised_Vol'].median(),
        'Median_SP500_Annualised_Vol': subset['SP500_Annualised_Vol'].median(),
        'Avg_Correlation': subset['Correlation'].mean(),
        'Median_Correlation': subset['Correlation'].median(),
        'Pct_Negative_Correlation': (subset['Correlation'] < 0).sum() / len(subset),
        'Avg_Vol_Ratio': subset['Vol_Ratio_Gold_SP'].mean(),
        'Max_Gold_Vol': subset['Gold_Annualised_Vol'].max(),
        'Max_SP500_Vol': subset['SP500_Annualised_Vol'].max(),
        'Gold_Sharpe_Approx': subset['Gold_Annual_Return'].mean() / subset['Gold_Annualised_Vol'].mean() if subset['Gold_Annualised_Vol'].mean() != 0 else np.nan,
        'SP500_Sharpe_Approx': subset['SP500_Annual_Return'].mean() / subset['SP500_Annualised_Vol'].mean() if subset['SP500_Annualised_Vol'].mean() != 0 else np.nan,
    })

df_regime = pd.DataFrame(regime_summary)

# =============================================================================
# 5. ADDITIONAL: STRESS TESTING ANALYSIS
# =============================================================================
# When SP500 has negative annual returns, how does gold behave?
print("Computing stress test analysis...")

negative_sp_years = df_annual[df_annual['SP500_Annual_Return'] < 0].copy()
positive_sp_years = df_annual[df_annual['SP500_Annual_Return'] >= 0].copy()

# High vol years for SP500 (above median)
median_sp_vol = df_annual['SP500_Annualised_Vol'].median()
high_vol_years = df_annual[df_annual['SP500_Annualised_Vol'] > median_sp_vol].copy()
low_vol_years = df_annual[df_annual['SP500_Annualised_Vol'] <= median_sp_vol].copy()

# Quintile analysis: sort years by SP500 return, split into 5 groups
df_sorted = df_annual.sort_values('SP500_Annual_Return').reset_index(drop=True)
n = len(df_sorted)
quintile_size = n // 5

quintile_stats = []
for i in range(5):
    start_idx = i * quintile_size
    end_idx = (i + 1) * quintile_size if i < 4 else n
    q_data = df_sorted.iloc[start_idx:end_idx]
    labels = ['Worst 20%', '20-40%', '40-60%', '60-80%', 'Best 20%']
    quintile_stats.append({
        'SP500_Return_Quintile': labels[i],
        'N_Years': len(q_data),
        'Avg_SP500_Return': q_data['SP500_Annual_Return'].mean(),
        'Avg_Gold_Return': q_data['Gold_Annual_Return'].mean(),
        'Avg_Correlation': q_data['Correlation'].mean(),
        'Avg_Gold_Vol': q_data['Gold_Annualised_Vol'].mean(),
        'Avg_SP500_Vol': q_data['SP500_Annualised_Vol'].mean(),
        'Gold_Vol_Ratio': q_data['Vol_Ratio_Gold_SP'].mean(),
        'Years': ', '.join([str(y) for y in sorted(q_data['Year'].tolist())]),
    })

df_quintile = pd.DataFrame(quintile_stats)

# Conditional analysis
conditional_stats = []
for label, subset in [('SP500 Down Years', negative_sp_years),
                       ('SP500 Up Years', positive_sp_years),
                       ('High Vol Years (SP500 > Median)', high_vol_years),
                       ('Low Vol Years (SP500 <= Median)', low_vol_years)]:
    if len(subset) == 0:
        continue
    conditional_stats.append({
        'Condition': label,
        'N_Years': len(subset),
        'Avg_Gold_Return': subset['Gold_Annual_Return'].mean(),
        'Avg_SP500_Return': subset['SP500_Annual_Return'].mean(),
        'Avg_Gold_Vol': subset['Gold_Annualised_Vol'].mean(),
        'Avg_SP500_Vol': subset['SP500_Annualised_Vol'].mean(),
        'Avg_Correlation': subset['Correlation'].mean(),
        'Pct_Positive_Gold': (subset['Gold_Annual_Return'] > 0).sum() / len(subset),
        'Avg_Vol_Ratio': subset['Vol_Ratio_Gold_SP'].mean(),
    })

df_conditional = pd.DataFrame(conditional_stats)

# =============================================================================
# 6. ROLLING STATISTICS (5-year rolling windows)
# =============================================================================
print("Computing rolling statistics...")

rolling_window = 5
rolling_stats = []
for i in range(len(df_annual) - rolling_window + 1):
    window = df_annual.iloc[i:i + rolling_window]
    start_yr = int(window['Year'].iloc[0])
    end_yr = int(window['Year'].iloc[-1])

    # Get all daily returns in this window
    mask = (df_returns['Year'] >= start_yr) & (df_returns['Year'] <= end_yr)
    daily_window = df_returns[mask]
    merged = daily_window[['Gold_Return', 'SP500_Return']].dropna()

    rolling_corr = merged['Gold_Return'].corr(merged['SP500_Return']) if len(merged) > 10 else np.nan

    rolling_stats.append({
        'Window': f"{start_yr}-{end_yr}",
        'Centre_Year': (start_yr + end_yr) / 2,
        'Rolling_5Y_Correlation': rolling_corr,
        'Rolling_5Y_Avg_Gold_Vol': window['Gold_Annualised_Vol'].mean(),
        'Rolling_5Y_Avg_SP500_Vol': window['SP500_Annualised_Vol'].mean(),
        'Rolling_5Y_Gold_Return': window['Gold_Annual_Return'].mean(),
        'Rolling_5Y_SP500_Return': window['SP500_Annual_Return'].mean(),
        'Rolling_5Y_Vol_Ratio': window['Vol_Ratio_Gold_SP'].mean(),
    })

df_rolling = pd.DataFrame(rolling_stats)

# =============================================================================
# 7. CREATE THE ANALYSIS SPREADSHEET
# =============================================================================
print("Creating analysis spreadsheet...")

output_path = '/home/user/Gold_safe_haven/Gold_Safe_Haven_Analysis.xlsx'
writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
workbook = writer.book

# ---- FORMAT DEFINITIONS ----
fmt_title = workbook.add_format({
    'bold': True, 'font_size': 16, 'font_color': '#1B3A4B',
    'bottom': 2, 'bottom_color': '#C7963E'
})
fmt_subtitle = workbook.add_format({
    'bold': True, 'font_size': 13, 'font_color': '#C7963E',
    'bottom': 1, 'bottom_color': '#DDDDDD'
})
fmt_section = workbook.add_format({
    'bold': True, 'font_size': 11, 'font_color': '#1B3A4B',
    'bg_color': '#F5F0E6', 'bottom': 1
})
fmt_header = workbook.add_format({
    'bold': True, 'font_size': 10, 'bg_color': '#1B3A4B',
    'font_color': 'white', 'border': 1, 'text_wrap': True,
    'align': 'center', 'valign': 'vcenter'
})
fmt_header_gold = workbook.add_format({
    'bold': True, 'font_size': 10, 'bg_color': '#C7963E',
    'font_color': 'white', 'border': 1, 'text_wrap': True,
    'align': 'center', 'valign': 'vcenter'
})
fmt_header_sp = workbook.add_format({
    'bold': True, 'font_size': 10, 'bg_color': '#2E5090',
    'font_color': 'white', 'border': 1, 'text_wrap': True,
    'align': 'center', 'valign': 'vcenter'
})
fmt_pct = workbook.add_format({
    'num_format': '0.00%', 'border': 1, 'align': 'center'
})
fmt_pct1 = workbook.add_format({
    'num_format': '0.0%', 'border': 1, 'align': 'center'
})
fmt_num2 = workbook.add_format({
    'num_format': '0.00', 'border': 1, 'align': 'center'
})
fmt_num0 = workbook.add_format({
    'num_format': '#,##0', 'border': 1, 'align': 'center'
})
fmt_price = workbook.add_format({
    'num_format': '#,##0.00', 'border': 1, 'align': 'center'
})
fmt_int = workbook.add_format({
    'num_format': '0', 'border': 1, 'align': 'center'
})
fmt_text = workbook.add_format({
    'text_wrap': True, 'valign': 'top', 'font_size': 10,
    'border': 0
})
fmt_text_bold = workbook.add_format({
    'text_wrap': True, 'valign': 'top', 'font_size': 10,
    'bold': True, 'border': 0
})
fmt_body = workbook.add_format({
    'text_wrap': True, 'valign': 'top', 'font_size': 10,
    'border': 0, 'font_color': '#333333'
})
fmt_finding = workbook.add_format({
    'text_wrap': True, 'valign': 'top', 'font_size': 10,
    'border': 0, 'font_color': '#8B0000', 'bold': True
})
fmt_cell = workbook.add_format({
    'border': 1, 'align': 'center', 'valign': 'vcenter'
})
fmt_cell_wrap = workbook.add_format({
    'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True
})
# Conditional: negative correlation (green = good hedge)
fmt_neg_corr = workbook.add_format({
    'num_format': '0.00', 'border': 1, 'align': 'center',
    'bg_color': '#C6EFCE', 'font_color': '#006100'
})
# Conditional: positive correlation (red = poor hedge)
fmt_pos_corr = workbook.add_format({
    'num_format': '0.00', 'border': 1, 'align': 'center',
    'bg_color': '#FFC7CE', 'font_color': '#9C0006'
})
# Gold vol > SP vol
fmt_high_vol = workbook.add_format({
    'num_format': '0.00%', 'border': 1, 'align': 'center',
    'bg_color': '#FFE699'
})

# =========================================================
# TAB 1: EXECUTIVE SUMMARY & DISCUSSION
# =========================================================
ws1 = workbook.add_worksheet('Executive Summary')
ws1.hide_gridlines(2)
ws1.set_column('A:A', 3)
ws1.set_column('B:B', 100)

row = 1
ws1.merge_range('B2:B2', 'Gold: Safe Haven or Volatile Mirage?', fmt_title)
row = 3
ws1.write(row, 1, 'An Empirical Analysis of Gold vs. S&P 500 (1971-2025)', fmt_subtitle)
row = 5

# Context
ws1.write(row, 1, 'BACKGROUND', fmt_section)
row += 1
ws1.set_row(row, 80)
ws1.write(row, 1,
    'Since the collapse of the Bretton Woods system in August 1971, gold has traded freely as a market-priced commodity. '
    'The conventional wisdom holds that gold is the quintessential "safe haven" - an asset that preserves value and '
    'offers portfolio protection when equity markets tumble. This narrative is deeply embedded in investment culture: '
    'when uncertainty rises, investors flock to gold.\n\n'
    'But "safe haven" and "safe asset" are not synonymous. A safe haven need only be uncorrelated or negatively '
    'correlated with risk assets during stress. A safe asset must also exhibit low volatility - it must be stable in its own right. '
    'Our analysis examines whether gold satisfies both conditions.',
    fmt_body)
row += 2

ws1.write(row, 1, 'THE THESIS', fmt_section)
row += 1
ws1.set_row(row, 60)
ws1.write(row, 1,
    'Gold may be perceived as a safe haven, but it is far from a safe asset. Its volatility surges dramatically '
    'during periods of market stress - precisely when investors flock to it most. The very act of seeking shelter in gold '
    'contributes to the instability that undermines its protective role. In the post-2005 era, gold has increasingly behaved '
    'like a risk asset, with its annualised volatility frequently exceeding that of the S&P 500.',
    fmt_finding)
row += 2

# Key findings
ws1.write(row, 1, 'KEY FINDINGS', fmt_section)
row += 1

# Compute key stats for findings
avg_corr_full = df_annual['Correlation'].mean()
avg_corr_pre = pre2005['Correlation'].mean()
avg_corr_post = post2005['Correlation'].mean()
pct_neg_pre = (pre2005['Correlation'] < 0).sum() / len(pre2005) * 100
pct_neg_post = (post2005['Correlation'] < 0).sum() / len(post2005) * 100
avg_gold_vol_pre = pre2005['Gold_Annualised_Vol'].mean()
avg_gold_vol_post = post2005['Gold_Annualised_Vol'].mean()
avg_sp_vol_pre = pre2005['SP500_Annualised_Vol'].mean()
avg_sp_vol_post = post2005['SP500_Annualised_Vol'].mean()
yrs_gold_higher_vol = (df_annual['Gold_Annualised_Vol'] > df_annual['SP500_Annualised_Vol']).sum()
yrs_gold_higher_vol_post = (post2005['Gold_Annualised_Vol'] > post2005['SP500_Annualised_Vol']).sum()

findings = [
    f'1. HEDGING DYNAMICS ARE ERODING: The average annual correlation between gold and S&P 500 returns was '
    f'{avg_corr_pre:.2f} in the stable era (1971-2005) vs. {avg_corr_post:.2f} in the volatile era (2006-2025). '
    f'Negative correlations - the hallmark of a hedge - occurred in {pct_neg_pre:.0f}% of years pre-2005 '
    f'but only {pct_neg_post:.0f}% post-2005.',

    f'2. GOLD VOLATILITY SURGES DURING STRESS: Gold\'s average annualised volatility rose from '
    f'{avg_gold_vol_pre:.1%} (1971-2005) to {avg_gold_vol_post:.1%} (2006-2025). '
    f'During the worst S&P 500 years, gold volatility averaged '
    f'{df_quintile[df_quintile["SP500_Return_Quintile"]=="Worst 20%"]["Avg_Gold_Vol"].values[0]:.1%} - '
    f'hardly the behaviour of a "safe" asset.',

    f'3. GOLD IS NOT A LOW-VOLATILITY ASSET: In {yrs_gold_higher_vol} out of {len(df_annual)} years, '
    f'gold\'s annualised volatility exceeded that of the S&P 500. '
    f'In the post-2005 volatile era, this occurred in {yrs_gold_higher_vol_post} out of {len(post2005)} years.',

    f'4. CONDITIONAL PERFORMANCE IS MIXED: In years when the S&P 500 posted negative returns, '
    f'gold delivered a positive return {(negative_sp_years["Gold_Annual_Return"] > 0).sum()}/{len(negative_sp_years)} times '
    f'({(negative_sp_years["Gold_Annual_Return"] > 0).sum()/len(negative_sp_years):.0%}). '
    f'While this supports the hedge narrative, the accompanying gold volatility of '
    f'{negative_sp_years["Gold_Annualised_Vol"].mean():.1%} annualised means that "protection" comes with '
    f'substantial price uncertainty.',

    f'5. THE COVID-19 LITMUS TEST: In 2020, when investors needed a safe haven most, '
    f'gold posted a correlation of {df_annual[df_annual["Year"]==2020]["Correlation"].values[0]:.2f} with the S&P 500 '
    f'and exhibited annualised volatility of {df_annual[df_annual["Year"]==2020]["Gold_Annualised_Vol"].values[0]:.1%}. '
    f'This echoes findings from Faraj et al. (2025) showing gold failed as a safe haven during COVID-19.',
]

for finding in findings:
    ws1.set_row(row, 50)
    ws1.write(row, 1, finding, fmt_body)
    row += 1

row += 1
ws1.write(row, 1, 'IMPLICATIONS FOR INVESTORS', fmt_section)
row += 1
ws1.set_row(row, 80)
ws1.write(row, 1,
    'The distinction between "safe haven" and "safe asset" is critical for portfolio construction. '
    'While gold has historically delivered positive returns in some equity market downturns, it does so with '
    'a level of volatility that can rival or exceed equities themselves. Investors who allocate to gold expecting '
    'stability may be surprised by the magnitude of gold price swings during the very crises they are seeking '
    'protection from.\n\n'
    'As Faraj, McMillan & Al-Sabah (2025) conclude in the reference article: "investors should be aware of gold\'s '
    'positive correlation with the stock market during periods of market stress and increased volatility. '
    'As such, adding gold to a portfolio may raise volatility without providing expected protection."',
    fmt_body)
row += 2

ws1.write(row, 1, 'METHODOLOGY', fmt_section)
row += 1
ws1.set_row(row, 80)
ws1.write(row, 1,
    'Data: Daily gold prices (USD/Troy Oz) and S&P 500 price index from August 1971 to February 2025, '
    'sourced via Datastream.\n'
    'Returns: Pre-computed daily natural log returns [ln(Pt/Pt-1)].\n'
    'Annual Correlation: Pearson correlation of daily log returns within each calendar year.\n'
    'Annual Volatility: Standard deviation of daily log returns, annualised by multiplying by sqrt(252).\n'
    'Annual Return: Sum of daily log returns within each calendar year (property of log returns).\n'
    'Regime Split: Pre-2005 ("stable era") vs. Post-2005 ("volatile era"), following the structural break '
    'identified by Faraj et al. (2025) around April 2006.',
    fmt_body)
row += 2

ws1.write(row, 1,
    'Reference: Faraj, H., McMillan, D. & Al-Sabah, M. (2025). "The diminishing lustre: Gold\'s market '
    'volatility and the fading safe haven effect." Global Finance Journal, 67, 101145.',
    fmt_text)

# =========================================================
# TAB 2: ANNUAL STATISTICS
# =========================================================
ws2 = workbook.add_worksheet('Annual Statistics')
ws2.hide_gridlines(2)
ws2.freeze_panes(3, 1)

# Title
ws2.merge_range('A1:P1', 'Annual Statistics: Gold vs. S&P 500 (1971-2025)', fmt_title)

# Headers
headers = [
    ('Year', fmt_header, 6),
    ('Obs', fmt_header, 5),
    ('Gold\nAnnual Return', fmt_header_gold, 12),
    ('S&P 500\nAnnual Return', fmt_header_sp, 12),
    ('Gold\nDaily Vol', fmt_header_gold, 10),
    ('S&P 500\nDaily Vol', fmt_header_sp, 10),
    ('Gold\nAnnualised Vol', fmt_header_gold, 13),
    ('S&P 500\nAnnualised Vol', fmt_header_sp, 13),
    ('Correlation\n(Gold-SP500)', fmt_header, 12),
    ('Vol Ratio\n(Gold/SP)', fmt_header, 10),
    ('Gold Max\nDrawdown', fmt_header_gold, 10),
    ('SP500 Max\nDrawdown', fmt_header_sp, 10),
    ('Gold\nStart Price', fmt_header_gold, 10),
    ('Gold\nEnd Price', fmt_header_gold, 10),
    ('SP500\nStart Price', fmt_header_sp, 10),
    ('SP500\nEnd Price', fmt_header_sp, 10),
]

for col, (name, fmt, width) in enumerate(headers):
    ws2.write(2, col, name, fmt)
    ws2.set_column(col, col, width)

ws2.set_row(2, 35)

# Data rows
for i, row_data in df_annual.iterrows():
    r = i + 3
    ws2.write(r, 0, int(row_data['Year']), fmt_int)
    ws2.write(r, 1, int(row_data['N_Obs']), fmt_int)
    ws2.write(r, 2, row_data['Gold_Annual_Return'], fmt_pct)
    ws2.write(r, 3, row_data['SP500_Annual_Return'], fmt_pct)
    ws2.write(r, 4, row_data['Gold_Daily_Vol'], fmt_pct)
    ws2.write(r, 5, row_data['SP500_Daily_Vol'], fmt_pct)
    ws2.write(r, 6, row_data['Gold_Annualised_Vol'], fmt_pct)
    ws2.write(r, 7, row_data['SP500_Annualised_Vol'], fmt_pct)

    # Color-code correlation
    corr_val = row_data['Correlation']
    if pd.notna(corr_val):
        corr_fmt = fmt_neg_corr if corr_val < 0 else fmt_pos_corr
        ws2.write(r, 8, corr_val, corr_fmt)
    else:
        ws2.write(r, 8, '', fmt_cell)

    ws2.write(r, 9, row_data['Vol_Ratio_Gold_SP'], fmt_num2)
    ws2.write(r, 10, row_data['Gold_Max_Drawdown'], fmt_pct)
    ws2.write(r, 11, row_data['SP500_Max_Drawdown'], fmt_pct)
    ws2.write(r, 12, row_data['Gold_Start_Price'], fmt_price)
    ws2.write(r, 13, row_data['Gold_End_Price'], fmt_price)
    ws2.write(r, 14, row_data['SP500_Start_Price'], fmt_price)
    ws2.write(r, 15, row_data['SP500_End_Price'], fmt_price)

# =========================================================
# TAB 3: CORRELATION ANALYSIS
# =========================================================
ws3 = workbook.add_worksheet('Correlation Analysis')
ws3.hide_gridlines(2)
ws3.set_column('A:A', 8)
ws3.set_column('B:B', 14)
ws3.set_column('C:C', 60)

ws3.merge_range('A1:C1', 'Annual Correlation: Gold vs. S&P 500 Daily Returns', fmt_title)

row = 3
ws3.write(row, 0, 'DISCUSSION', fmt_section)
row += 1
ws3.set_row(row, 100)
ws3.merge_range(row, 0, row, 2,
    'The annual correlation between daily gold and S&P 500 log returns reveals the time-varying nature of gold\'s hedging properties. '
    'A negative correlation indicates gold moves inversely to equities - the classic "safe haven" behaviour. '
    'A positive correlation means gold co-moves with equities, undermining its hedging role.\n\n'
    'KEY OBSERVATION: The correlation fluctuates substantially from year to year, ranging from strongly negative to '
    'strongly positive. There is no persistent negative correlation that would justify treating gold as a reliable hedge. '
    'In the volatile era (post-2005), the correlation has trended upward, with gold increasingly co-moving with equities. '
    'This is consistent with Faraj et al. (2025) who find that gold displays a positive correlation with the S&P 500 '
    'in most high-volatility periods after 2005.',
    fmt_body)

row += 2
ws3.write(row, 0, 'Year', fmt_header)
ws3.write(row, 1, 'Correlation', fmt_header)
ws3.write(row, 2, 'Interpretation', fmt_header)

for i, rd in df_annual.iterrows():
    row += 1
    ws3.write(row, 0, int(rd['Year']), fmt_int)
    corr_val = rd['Correlation']
    if pd.notna(corr_val):
        corr_fmt = fmt_neg_corr if corr_val < 0 else fmt_pos_corr
        ws3.write(row, 1, corr_val, corr_fmt)
        if corr_val < -0.2:
            interp = 'Strong hedge: gold moves significantly inversely to equities'
        elif corr_val < -0.05:
            interp = 'Moderate hedge: mild inverse relationship'
        elif corr_val < 0.05:
            interp = 'Near-zero: no meaningful linear relationship (weak safe haven at best)'
        elif corr_val < 0.2:
            interp = 'Moderate co-movement: gold offers diversification only, not a hedge'
        else:
            interp = 'Strong co-movement: gold moves with equities - NO hedge/safe haven'
    else:
        ws3.write(row, 1, '', fmt_cell)
        interp = 'Insufficient data'
    ws3.write(row, 2, interp, fmt_cell_wrap)

# =========================================================
# TAB 4: VOLATILITY COMPARISON
# =========================================================
ws4 = workbook.add_worksheet('Volatility Comparison')
ws4.hide_gridlines(2)
ws4.set_column('A:A', 8)
ws4.set_column('B:C', 16)
ws4.set_column('D:D', 12)
ws4.set_column('E:E', 60)

ws4.merge_range('A1:E1', 'Annualised Volatility: Gold vs. S&P 500', fmt_title)

row = 3
ws4.write(row, 0, 'DISCUSSION', fmt_section)
row += 1
ws4.set_row(row, 120)
ws4.merge_range(row, 0, row, 4,
    'A truly "safe" asset should exhibit low and stable volatility. This tab compares the annualised volatility '
    '(standard deviation of daily log returns x sqrt(252)) of gold and the S&P 500 each year.\n\n'
    'KEY OBSERVATION: Gold\'s volatility is NOT consistently lower than equities. In many years, gold is MORE volatile '
    'than the S&P 500. The volatility ratio (Gold/SP500) frequently exceeds 1.0, meaning gold is the riskier asset '
    'on a standalone basis.\n\n'
    f'STATISTICS: Over the full sample, gold\'s annualised volatility exceeded that of the S&P 500 in '
    f'{yrs_gold_higher_vol} out of {len(df_annual)} years ({yrs_gold_higher_vol/len(df_annual):.0%}). '
    f'The average annualised volatility was {df_annual["Gold_Annualised_Vol"].mean():.1%} for gold vs. '
    f'{df_annual["SP500_Annualised_Vol"].mean():.1%} for the S&P 500.\n\n'
    'CRITICAL INSIGHT: During periods of equity market stress (when investors allegedly "flee to gold"), gold volatility '
    'spikes as well. This means investors exchanging equities for gold are not reducing their exposure to price uncertainty - '
    'they may simply be swapping one volatile asset for another.',
    fmt_body)

row += 2
ws4.write(row, 0, 'Year', fmt_header)
ws4.write(row, 1, 'Gold\nAnnualised Vol', fmt_header_gold)
ws4.write(row, 2, 'S&P 500\nAnnualised Vol', fmt_header_sp)
ws4.write(row, 3, 'Vol Ratio\n(Gold/SP)', fmt_header)
ws4.write(row, 4, 'Assessment', fmt_header)
ws4.set_row(row, 30)

for i, rd in df_annual.iterrows():
    row += 1
    ws4.write(row, 0, int(rd['Year']), fmt_int)

    g_vol = rd['Gold_Annualised_Vol']
    s_vol = rd['SP500_Annualised_Vol']
    ratio = rd['Vol_Ratio_Gold_SP']

    vol_fmt_g = fmt_high_vol if g_vol > s_vol else fmt_pct
    ws4.write(row, 1, g_vol, vol_fmt_g)
    ws4.write(row, 2, s_vol, fmt_pct)
    ws4.write(row, 3, ratio, fmt_num2)

    if ratio > 1.5:
        assess = 'Gold MUCH more volatile than SP500 - undermines "safe asset" narrative'
    elif ratio > 1.0:
        assess = 'Gold more volatile than SP500 - not a low-risk alternative'
    elif ratio > 0.8:
        assess = 'Similar volatility levels - gold offers no volatility reduction'
    else:
        assess = 'Gold less volatile - consistent with safe asset characteristics'
    ws4.write(row, 4, assess, fmt_cell_wrap)

# =========================================================
# TAB 5: REGIME ANALYSIS
# =========================================================
ws5 = workbook.add_worksheet('Regime Analysis')
ws5.hide_gridlines(2)
ws5.set_column('A:A', 30)
ws5.set_column('B:R', 16)

ws5.merge_range('A1:H1', 'Regime Analysis: Stable Era (1971-2005) vs. Volatile Era (2006-2025)', fmt_title)

row = 3
ws5.write(row, 0, 'DISCUSSION', fmt_section)
row += 1
ws5.set_row(row, 100)
ws5.merge_range(row, 0, row, 7,
    'Following the structural break identified around 2005/2006 (see Faraj et al. 2025), we split the sample into two regimes. '
    'The "Stable Era" (1971-2005) covers the post-Bretton Woods transition, OPEC shocks, and a long period of relative gold price stability '
    '(1985-2005, when gold traded around $300-$450). The "Volatile Era" (2006-2025) covers the financialisation of gold, '
    'the GFC, European debt crisis, COVID-19, and the recent gold surge past $2,700.\n\n'
    'KEY FINDING: The data reveals a stark contrast between eras. In the stable era, gold exhibited lower volatility, '
    'more frequent negative correlations with equities, and classic safe haven behaviour. In the volatile era, gold\'s volatility '
    'surged, correlations turned less consistently negative, and gold increasingly co-moved with equities during crises.',
    fmt_body)

row += 2
regime_headers = [
    'Metric', 'Full Sample\n(1971-2025)', 'Stable Era\n(1971-2005)', 'Volatile Era\n(2006-2025)'
]
for col, h in enumerate(regime_headers):
    ws5.write(row, col, h, fmt_header)
    if col > 0:
        ws5.set_column(col, col, 18)

row += 1
regime_metrics = [
    ('Number of Years', 'N_Years', fmt_int),
    ('Avg Gold Annual Return', 'Avg_Gold_Return', fmt_pct),
    ('Avg S&P 500 Annual Return', 'Avg_SP500_Return', fmt_pct),
    ('Median Gold Annual Return', 'Median_Gold_Return', fmt_pct),
    ('Median S&P 500 Annual Return', 'Median_SP500_Return', fmt_pct),
    ('Avg Gold Annualised Volatility', 'Avg_Gold_Annualised_Vol', fmt_pct),
    ('Avg S&P 500 Annualised Volatility', 'Avg_SP500_Annualised_Vol', fmt_pct),
    ('Median Gold Annualised Volatility', 'Median_Gold_Annualised_Vol', fmt_pct),
    ('Median S&P 500 Annualised Volatility', 'Median_SP500_Annualised_Vol', fmt_pct),
    ('Average Annual Correlation', 'Avg_Correlation', fmt_num2),
    ('Median Annual Correlation', 'Median_Correlation', fmt_num2),
    ('% Years with Negative Correlation', 'Pct_Negative_Correlation', fmt_pct1),
    ('Average Volatility Ratio (Gold/SP)', 'Avg_Vol_Ratio', fmt_num2),
    ('Maximum Gold Annualised Volatility', 'Max_Gold_Vol', fmt_pct),
    ('Maximum S&P 500 Annualised Volatility', 'Max_SP500_Vol', fmt_pct),
    ('Approx. Sharpe Ratio (Gold)', 'Gold_Sharpe_Approx', fmt_num2),
    ('Approx. Sharpe Ratio (S&P 500)', 'SP500_Sharpe_Approx', fmt_num2),
]

for metric_name, col_name, fmt in regime_metrics:
    ws5.write(row, 0, metric_name, fmt_text_bold)
    for j in range(3):
        val = df_regime.iloc[j][col_name]
        ws5.write(row, j + 1, val, fmt)
    row += 1

# =========================================================
# TAB 6: CRISIS PERIODS
# =========================================================
ws6 = workbook.add_worksheet('Crisis Periods')
ws6.hide_gridlines(2)
ws6.set_column('A:A', 30)
ws6.set_column('B:B', 12)
ws6.set_column('C:C', 50)
ws6.set_column('D:I', 16)

ws6.merge_range('A1:I1', 'Gold Behaviour During Market Crises (1971-2025)', fmt_title)

row = 3
ws6.write(row, 0, 'DISCUSSION', fmt_section)
row += 1
ws6.set_row(row, 80)
ws6.merge_range(row, 0, row, 8,
    'This tab examines gold\'s behaviour during specific crisis periods. For a true safe haven, we would expect: '
    '(1) negative correlation with equities, (2) positive gold returns, and (3) moderate gold volatility. '
    'The evidence is mixed. While gold sometimes delivered positive returns during equity downturns, it also '
    'exhibited elevated volatility and, in several recent crises, positive correlations with equities.\n\n'
    'NOTE: The 2007-2009 GFC is particularly instructive. Gold fell sharply alongside equities in late 2008, '
    'with a positive correlation of 6.4% with stocks during the ICSS-identified volatility period (per Faraj et al. 2025). '
    'The COVID-19 crash of March 2020 showed similar behaviour, with gold falling ~15% from its February peak.',
    fmt_body)

row += 2
crisis_headers = ['Period', 'Years', 'Description', 'Avg\nCorrelation', 'Gold\nCum. Return',
                   'SP500\nCum. Return', 'Gold\nAvg Ann. Vol', 'SP500\nAvg Ann. Vol', 'Avg Vol\nRatio']
for col, h in enumerate(crisis_headers):
    ws6.write(row, col, h, fmt_header)
ws6.set_row(row, 30)

for i, rd in df_crisis.iterrows():
    row += 1
    ws6.write(row, 0, rd['Period'], fmt_cell_wrap)
    ws6.write(row, 1, rd['Years'], fmt_cell)
    ws6.write(row, 2, rd['Description'], fmt_cell_wrap)
    ws6.set_row(row, 35)

    corr_val = rd['Avg_Correlation']
    corr_fmt = fmt_neg_corr if corr_val < 0 else fmt_pos_corr
    ws6.write(row, 3, corr_val, corr_fmt)

    ws6.write(row, 4, rd['Gold_Cumulative_Return'], fmt_pct)
    ws6.write(row, 5, rd['SP500_Cumulative_Return'], fmt_pct)
    ws6.write(row, 6, rd['Gold_Avg_Annual_Vol'], fmt_pct)
    ws6.write(row, 7, rd['SP500_Avg_Annual_Vol'], fmt_pct)
    ws6.write(row, 8, rd['Avg_Vol_Ratio'], fmt_num2)

# =========================================================
# TAB 7: CONDITIONAL ANALYSIS
# =========================================================
ws7 = workbook.add_worksheet('Conditional Analysis')
ws7.hide_gridlines(2)
ws7.set_column('A:A', 35)
ws7.set_column('B:I', 14)

ws7.merge_range('A1:I1', 'Conditional Analysis: Gold Behaviour by S&P 500 Regime', fmt_title)

row = 3
ws7.write(row, 0, 'DISCUSSION', fmt_section)
row += 1
ws7.set_row(row, 80)
ws7.merge_range(row, 0, row, 8,
    'If gold is a reliable safe haven, it should deliver its best performance (positive returns, low volatility, negative correlation) '
    'precisely when equities perform worst. This tab tests that proposition by conditioning on equity market outcomes.\n\n'
    'SECTION A groups years by whether the S&P 500 was up or down, and whether equity volatility was above or below median.\n'
    'SECTION B sorts all years into quintiles by S&P 500 annual return, from worst to best.\n\n'
    'KEY FINDING: Gold does tend to post positive returns during equity down years, but its volatility also rises sharply. '
    'The "protection" gold offers comes bundled with substantial price uncertainty.',
    fmt_body)

# Section A: Conditional stats
row += 2
ws7.write(row, 0, 'SECTION A: Conditional Statistics', fmt_subtitle)
row += 1
cond_headers = ['Condition', 'N Years', 'Avg Gold\nReturn', 'Avg SP500\nReturn',
                'Avg Gold\nAnn. Vol', 'Avg SP500\nAnn. Vol', 'Avg\nCorrelation',
                '% Gold\nPositive', 'Avg Vol\nRatio']
for col, h in enumerate(cond_headers):
    ws7.write(row, col, h, fmt_header)
ws7.set_row(row, 30)

for i, rd in df_conditional.iterrows():
    row += 1
    ws7.write(row, 0, rd['Condition'], fmt_cell_wrap)
    ws7.write(row, 1, int(rd['N_Years']), fmt_int)
    ws7.write(row, 2, rd['Avg_Gold_Return'], fmt_pct)
    ws7.write(row, 3, rd['Avg_SP500_Return'], fmt_pct)
    ws7.write(row, 4, rd['Avg_Gold_Vol'], fmt_pct)
    ws7.write(row, 5, rd['Avg_SP500_Vol'], fmt_pct)
    corr_val = rd['Avg_Correlation']
    corr_fmt = fmt_neg_corr if corr_val < 0 else fmt_pos_corr
    ws7.write(row, 6, corr_val, corr_fmt)
    ws7.write(row, 7, rd['Pct_Positive_Gold'], fmt_pct1)
    ws7.write(row, 8, rd['Avg_Vol_Ratio'], fmt_num2)

# Section B: Quintile analysis
row += 3
ws7.write(row, 0, 'SECTION B: Quintile Analysis (by S&P 500 Annual Return)', fmt_subtitle)
row += 1
quint_headers = ['SP500 Quintile', 'N Years', 'Avg SP500\nReturn', 'Avg Gold\nReturn',
                 'Avg\nCorrelation', 'Avg Gold\nAnn. Vol', 'Avg SP500\nAnn. Vol',
                 'Vol Ratio\n(Gold/SP)', 'Years']
for col, h in enumerate(quint_headers):
    ws7.write(row, col, h, fmt_header)
    if col == 8:
        ws7.set_column(col, col, 50)
ws7.set_row(row, 30)

for i, rd in df_quintile.iterrows():
    row += 1
    ws7.write(row, 0, rd['SP500_Return_Quintile'], fmt_cell)
    ws7.write(row, 1, int(rd['N_Years']), fmt_int)
    ws7.write(row, 2, rd['Avg_SP500_Return'], fmt_pct)
    ws7.write(row, 3, rd['Avg_Gold_Return'], fmt_pct)
    corr_val = rd['Avg_Correlation']
    corr_fmt = fmt_neg_corr if corr_val < 0 else fmt_pos_corr
    ws7.write(row, 4, corr_val, corr_fmt)
    ws7.write(row, 5, rd['Avg_Gold_Vol'], fmt_pct)
    ws7.write(row, 6, rd['Avg_SP500_Vol'], fmt_pct)
    ws7.write(row, 7, rd['Gold_Vol_Ratio'], fmt_num2)
    ws7.write(row, 8, rd['Years'], fmt_cell_wrap)
    ws7.set_row(row, 30)

# =========================================================
# TAB 8: ROLLING ANALYSIS
# =========================================================
ws8 = workbook.add_worksheet('Rolling 5-Year Analysis')
ws8.hide_gridlines(2)
ws8.set_column('A:A', 14)
ws8.set_column('B:G', 16)

ws8.merge_range('A1:G1', '5-Year Rolling Window Analysis', fmt_title)

row = 3
ws8.write(row, 0, 'DISCUSSION', fmt_section)
row += 1
ws8.set_row(row, 80)
ws8.merge_range(row, 0, row, 6,
    'Rolling 5-year windows smooth out year-to-year noise and reveal the underlying trend in the gold-equity relationship. '
    'The rolling correlation captures the medium-term hedging dynamic: a persistently negative rolling correlation would '
    'support gold\'s role as a structural hedge.\n\n'
    'KEY OBSERVATION: The rolling 5-year correlation was predominantly negative through the 1990s and early 2000s, '
    'but has since fluctuated between negative and positive territory. The rolling gold volatility has also trended upward, '
    'while its return advantage over equities has narrowed in many periods.',
    fmt_body)

row += 2
roll_headers = ['Window', '5Y Rolling\nCorrelation', '5Y Avg Gold\nAnn. Vol', '5Y Avg SP500\nAnn. Vol',
                '5Y Avg Gold\nReturn', '5Y Avg SP500\nReturn', '5Y Avg Vol\nRatio']
for col, h in enumerate(roll_headers):
    ws8.write(row, col, h, fmt_header)
ws8.set_row(row, 30)

for i, rd in df_rolling.iterrows():
    row += 1
    ws8.write(row, 0, rd['Window'], fmt_cell)
    corr_val = rd['Rolling_5Y_Correlation']
    if pd.notna(corr_val):
        corr_fmt = fmt_neg_corr if corr_val < 0 else fmt_pos_corr
        ws8.write(row, 1, corr_val, corr_fmt)
    else:
        ws8.write(row, 1, '', fmt_cell)
    ws8.write(row, 2, rd['Rolling_5Y_Avg_Gold_Vol'], fmt_pct)
    ws8.write(row, 3, rd['Rolling_5Y_Avg_SP500_Vol'], fmt_pct)
    ws8.write(row, 4, rd['Rolling_5Y_Gold_Return'], fmt_pct)
    ws8.write(row, 5, rd['Rolling_5Y_SP500_Return'], fmt_pct)
    ws8.write(row, 6, rd['Rolling_5Y_Vol_Ratio'], fmt_num2)

# =========================================================
# TAB 9: CHARTS DATA (formatted for easy charting)
# =========================================================
ws9 = workbook.add_worksheet('Chart Data')
ws9.hide_gridlines(2)

ws9.merge_range('A1:J1', 'Data Prepared for Charting', fmt_title)

row = 3
chart_headers = ['Year', 'Gold Ann. Return', 'SP500 Ann. Return',
                 'Gold Ann. Vol', 'SP500 Ann. Vol',
                 'Correlation', 'Vol Ratio',
                 'Gold Price (YE)', 'SP500 Price (YE)',
                 'Gold Cumulative Return']

for col, h in enumerate(chart_headers):
    ws9.write(row, col, h, fmt_header)
    ws9.set_column(col, col, 15)

# Compute cumulative return for gold
gold_cum_return = 0
for i, rd in df_annual.iterrows():
    row += 1
    gold_cum_return += rd['Gold_Annual_Return']
    ws9.write(row, 0, int(rd['Year']), fmt_int)
    ws9.write(row, 1, rd['Gold_Annual_Return'], fmt_pct)
    ws9.write(row, 2, rd['SP500_Annual_Return'], fmt_pct)
    ws9.write(row, 3, rd['Gold_Annualised_Vol'], fmt_pct)
    ws9.write(row, 4, rd['SP500_Annualised_Vol'], fmt_pct)
    ws9.write(row, 5, rd['Correlation'], fmt_num2)
    ws9.write(row, 6, rd['Vol_Ratio_Gold_SP'], fmt_num2)
    ws9.write(row, 7, rd['Gold_End_Price'], fmt_price)
    ws9.write(row, 8, rd['SP500_End_Price'], fmt_price)
    ws9.write(row, 9, gold_cum_return, fmt_pct)

# =========================================================
# ADD CHARTS
# =========================================================
print("Adding charts...")

n_years = len(df_annual)
data_start_row = 4  # 0-indexed row where data starts in Chart Data tab
data_end_row = data_start_row + n_years - 1

# Chart 1: Annual Correlation Time Series
chart1 = workbook.add_chart({'type': 'column'})
chart1.add_series({
    'name': 'Gold-SP500 Correlation',
    'categories': ['Chart Data', data_start_row, 0, data_end_row, 0],
    'values': ['Chart Data', data_start_row, 5, data_end_row, 5],
    'fill': {'color': '#C7963E'},
    'border': {'color': '#C7963E'},
})
chart1.set_title({'name': 'Annual Correlation: Gold vs. S&P 500 Daily Returns'})
chart1.set_x_axis({'name': 'Year', 'num_font': {'size': 8, 'rotation': -45}})
chart1.set_y_axis({'name': 'Correlation', 'min': -1, 'max': 1})
chart1.set_size({'width': 900, 'height': 400})
chart1.set_legend({'none': True})
ws9.insert_chart('A' + str(data_end_row + 4), chart1)

# Chart 2: Annualised Volatility Comparison
chart2 = workbook.add_chart({'type': 'line'})
chart2.add_series({
    'name': 'Gold Annualised Vol',
    'categories': ['Chart Data', data_start_row, 0, data_end_row, 0],
    'values': ['Chart Data', data_start_row, 3, data_end_row, 3],
    'line': {'color': '#C7963E', 'width': 2},
})
chart2.add_series({
    'name': 'S&P 500 Annualised Vol',
    'categories': ['Chart Data', data_start_row, 0, data_end_row, 0],
    'values': ['Chart Data', data_start_row, 4, data_end_row, 4],
    'line': {'color': '#2E5090', 'width': 2},
})
chart2.set_title({'name': 'Annualised Volatility: Gold vs. S&P 500'})
chart2.set_x_axis({'name': 'Year', 'num_font': {'size': 8, 'rotation': -45}})
chart2.set_y_axis({'name': 'Annualised Volatility', 'num_format': '0%'})
chart2.set_size({'width': 900, 'height': 400})
ws9.insert_chart('A' + str(data_end_row + 25), chart2)

# Chart 3: Annual Returns Comparison
chart3 = workbook.add_chart({'type': 'column'})
chart3.add_series({
    'name': 'Gold Annual Return',
    'categories': ['Chart Data', data_start_row, 0, data_end_row, 0],
    'values': ['Chart Data', data_start_row, 1, data_end_row, 1],
    'fill': {'color': '#C7963E'},
})
chart3.add_series({
    'name': 'S&P 500 Annual Return',
    'categories': ['Chart Data', data_start_row, 0, data_end_row, 0],
    'values': ['Chart Data', data_start_row, 2, data_end_row, 2],
    'fill': {'color': '#2E5090'},
})
chart3.set_title({'name': 'Annual Log Returns: Gold vs. S&P 500'})
chart3.set_x_axis({'name': 'Year', 'num_font': {'size': 8, 'rotation': -45}})
chart3.set_y_axis({'name': 'Log Return', 'num_format': '0%'})
chart3.set_size({'width': 900, 'height': 400})
ws9.insert_chart('A' + str(data_end_row + 46), chart3)

# Chart 4: Volatility Ratio
chart4 = workbook.add_chart({'type': 'column'})
chart4.add_series({
    'name': 'Vol Ratio (Gold/SP500)',
    'categories': ['Chart Data', data_start_row, 0, data_end_row, 0],
    'values': ['Chart Data', data_start_row, 6, data_end_row, 6],
    'fill': {'color': '#8B4513'},
})
# Add a reference line at 1.0
chart4.set_title({'name': 'Volatility Ratio: Gold / S&P 500 (>1 = Gold More Volatile)'})
chart4.set_x_axis({'name': 'Year', 'num_font': {'size': 8, 'rotation': -45}})
chart4.set_y_axis({'name': 'Ratio'})
chart4.set_size({'width': 900, 'height': 400})
chart4.set_legend({'none': True})
ws9.insert_chart('A' + str(data_end_row + 67), chart4)

# =========================================================
# FINALIZE
# =========================================================
writer.close()
print(f"\nAnalysis spreadsheet saved to: {output_path}")
print("Done!")
