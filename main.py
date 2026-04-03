# main.py
# Главный файл

import asyncio
import pandas as pd
from collector import collect_data
# тональность:
from nlp_processor import identify_companies, analyze_social_reaction
# цены:
from market_data import get_moex_prices
# аналитика (графики):
from analytics import calculate_correlation, plot_results

async def main():
    print("=== Старт системы ===")
    
# ШАГ 1: Сбор данных (УКАЗЫВАЕМ канал и сколько постов хотим)
    channel = 'newssmartlab'
    # ЗАДАЁМ ограничение на сбор постов - 200 для теста (попробовать 20к)
    raw_df = await collect_data(channel, limit=20000)
    
    # print("\nПример собранных данных:")
    # print(raw_df.head(5))


# ШАГ 2: Идентификация компаний (NER)
    '''
    В текстах Smart-Lab обычно есть хештеги (#SBER, #GAZP).
    функция будет искать эти теги и сленг.
    '''
    df_with_entities = identify_companies(raw_df)
    
    if df_with_entities.empty:
        print("Компании не найдены. Слишком мало данных.")
        return
        
    print("\nПример найденных компаний:")
    print(df_with_entities[['ticker', 'text']].head(3))

# ШАГ 3: Классификация тональности (Моделирование отклика)
    '''
    Прогоняем текст через легковесную модель для предсказания 
    потенциальной реакции (позитив/негатив => покупка/продажа).
    '''
    df_scored = analyze_social_reaction(df_with_entities)
    
    # Выводим тикер, оценку и начало текста
    df_scored['short_text'] = df_scored['text'].str[:300] + "..."
    print("\nПример оценки реакции социума:")
    print(df_scored[['ticker', 'sentiment_score', 'short_text']].head(20))


# ШАГ 4: Сопоставление с котировками MOEX
    '''
    Скачиваем графики цен для найденных тикеров за те же даты.
    '''
    df_final = get_moex_prices(df_scored)

    if not df_final.empty:
        print("\nТаблица для анализа")
        # Выведем: дату, тикер, прогноз и реальное изменение цены
        print(df_final[['date', 'ticker', 'sentiment_score', 'day_return']].head(10))
        
        # Сохраним результат для финального шага
        
        df_final.to_csv('data/final_analysis_data.csv', index=False)
    

# ШАГ 5: Корреляция и визуализация
    '''
    # Рассчитываем коэффициент Пирсона и строим графики.
    '''
    correlation_results = calculate_correlation(df_final)
    plot_results(df_final, correlation_results)
    
    print("\n=== Работа системы завершена ===")

if __name__ == "__main__":
    asyncio.run(main())