# market_data.py
# Для получения цен акций

import pandas as pd
from moexalgo import Ticker
import time

def get_moex_prices(df: pd.DataFrame) -> pd.DataFrame:
    print("Запуск модуля MOEX (получение цен акций)")
    # Фильтруем тикеры (от 4 до 5 букв, только латиница)
    all_tickers = df['ticker'].unique()
    valid_tickers = [t for t in all_tickers if len(str(t)) >= 4 and str(t).isalpha()]
    # Форматируем даты
    all_market_data = []
    df['date_dt'] = pd.to_datetime(df['date'])
    start_str = df['date_dt'].min().strftime('%Y-%m-%d')
    end_str = df['date_dt'].max().strftime('%Y-%m-%d')

    for t_code in valid_tickers:
        try:
            t = Ticker(t_code)
            candles = t.candles(start=start_str, end=end_str, period='1D')
            candles_df = pd.DataFrame(candles)
            if candles_df.empty: continue
            # moexalgo возвращает колонки [open, close, begin, ...]
            candles_df['price_date'] = pd.to_datetime(candles_df['begin']).dt.date
            candles_df['ticker'] = t_code
            # Считаем доходность за день в %
            # (Цена закрытия - Цена открытия) / Цена открытия * 100
            candles_df['day_return'] = (candles_df['close'] - candles_df['open']) / candles_df['open'] * 100
            
            all_market_data.append(candles_df[['price_date', 'ticker', 'day_return', 'close']])
            time.sleep(0.2)     # Небольшая пауза, чтобы биржа не забанила за частые запросы
        except Exception as e:
            continue

    if not all_market_data: return pd.DataFrame()

    market_df = pd.concat(all_market_data)
    df['date_only'] = df['date_dt'].dt.date
    
    # Объединяем
    final_df = pd.merge(
        df, 
        market_df, 
        left_on=['date_only', 'ticker'], 
        right_on=['price_date', 'ticker'], 
        how='inner'
    )

    # Оставляем только 'date' (полная), 'ticker', 'text', 'sentiment_score' (если есть), 'day_return' и 'close'
    needed_columns = ['date', 'ticker', 'text', 'sentiment_score', 'day_return', 'close']
    final_df = final_df[[col for col in needed_columns if col in final_df.columns]]
    final_df = final_df.drop_duplicates()

    print(f"Готово. Сопоставлено записей: {len(final_df)}")
    return final_df
