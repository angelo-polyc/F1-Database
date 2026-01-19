import os
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

one_year_ago = datetime.now() - timedelta(days=365)

cur.execute("""
    SELECT pulled_at::date as date, value 
    FROM metrics 
    WHERE source = 'defillama' 
      AND asset = 'aave' 
      AND metric_name = 'REVENUE'
      AND pulled_at >= %s
    ORDER BY pulled_at
""", (one_year_ago,))
defillama_data = cur.fetchall()

cur.execute("""
    SELECT pulled_at::date as date, value 
    FROM metrics 
    WHERE source = 'artemis' 
      AND asset = 'aave' 
      AND metric_name = 'REVENUE'
      AND pulled_at >= %s
    ORDER BY pulled_at
""", (one_year_ago,))
artemis_data = cur.fetchall()

conn.close()

fig, axes = plt.subplots(2, 1, figsize=(14, 10))
fig.suptitle('Aave Protocol Daily Revenue (Past Year)', fontsize=16, fontweight='bold')

if defillama_data:
    dates = [row[0] for row in defillama_data]
    values = [row[1] / 1000 for row in defillama_data]
    
    axes[0].fill_between(dates, values, alpha=0.3, color='#1f77b4')
    axes[0].plot(dates, values, color='#1f77b4', linewidth=1)
    axes[0].set_title(f'DefiLlama Data ({len(defillama_data)} days)', fontsize=12)
    axes[0].set_ylabel('Daily Revenue ($K)', fontsize=10)
    axes[0].xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    axes[0].xaxis.set_major_locator(mdates.MonthLocator())
    axes[0].tick_params(axis='x', rotation=45)
    axes[0].grid(True, alpha=0.3)
    
    avg_revenue = sum(values) / len(values)
    axes[0].axhline(y=avg_revenue, color='red', linestyle='--', alpha=0.7, label=f'Avg: ${avg_revenue:.0f}K')
    axes[0].legend()

if artemis_data:
    dates = [row[0] for row in artemis_data]
    values = [row[1] / 1000 for row in artemis_data]
    
    axes[1].bar(dates, values, color='#ff7f0e', alpha=0.7, width=0.8)
    axes[1].set_title(f'Artemis Data ({len(artemis_data)} days available)', fontsize=12)
    axes[1].set_ylabel('Daily Revenue ($K)', fontsize=10)
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].grid(True, alpha=0.3)
else:
    axes[1].text(0.5, 0.5, 'Artemis: Only 5 recent data points\n(daily pulls started recently)', 
                 ha='center', va='center', transform=axes[1].transAxes, fontsize=12)
    axes[1].set_title('Artemis Data (Limited - Only Recent Pulls)', fontsize=12)

plt.tight_layout()
plt.savefig('aave_revenue_chart.png', dpi=150, bbox_inches='tight')
print('Chart saved to aave_revenue_chart.png')

total_revenue = sum(row[1] for row in defillama_data) / 1e6
avg_daily = total_revenue * 1e6 / len(defillama_data) / 1000
print(f'\nDefiLlama Summary:')
print(f'  Total Revenue (1yr): ${total_revenue:.2f}M')
print(f'  Average Daily: ${avg_daily:.0f}K')
print(f'  Data Points: {len(defillama_data)}')
