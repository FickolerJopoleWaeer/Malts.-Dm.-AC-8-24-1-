# analytics.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def calculate_correlation(df: pd.DataFrame):
    print("\n=== Расчет корреляции ===")
    
    # 1. Общая корреляция по всему рынку
    overall_corr = df['sentiment_score'].corr(df['day_return'])
    print(f"Общая корреляция (все акции): {overall_corr:.4f}")
    
    # 2. Корреляция по конкретным тикерам (где было хотя бы 3 новости)
    ticker_stats = df.groupby('ticker').agg(
        news_count=('text', 'count'),
        corr=('sentiment_score', lambda x: x.corr(df.loc[x.index, 'day_return']))
    ).dropna() # Удаляем NaN (где не хватило данных для расчета)
    
    # Оставляем только те акции, про которые писали хотя бы 3 раза: количество новостей и коэф кор выводим
    valid_tickers = ticker_stats[ticker_stats['news_count'] >= 3].sort_values(by='corr', ascending=False)
    
    if not valid_tickers.empty:
        print("\nТоп акций, реагирующих на новости (по Пирсону):")
        print(valid_tickers.head(10))
    else:
        print("\nСлишком мало данных для расчета корреляции по отдельным тикерам.")
        
    return valid_tickers

def plot_results(df: pd.DataFrame, ticker_stats: pd.DataFrame):
    print("Генерация графиков...")
    
    # Настраиваем стиль
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # График 1: Общее облако рассеяния с линией тренда
    sns.regplot(
        ax=axes[0],
        x='sentiment_score', 
        y='day_return', 
        data=df, 
        scatter_kws={'alpha':0.5, 'color': 'blue'}, 
        line_kws={'color': 'red'}
    )
    axes[0].set_title('Общая зависимость: Тональность vs Изменение цены (%)')
    axes[0].set_xlabel('Тональность новости (от -1.0 до 1.0)')
    axes[0].set_ylabel('Изменение цены за день (%)')
    
    # График 2: Топ-10 самых "чувствительных" акций
    if not ticker_stats.empty:
        top_10 = ticker_stats.head(10).reset_index()
        sns.barplot(
            ax=axes[1],
            x='ticker', 
            y='corr', 
            data=top_10,
            palette='viridis',
            hue='ticker',
            legend=False      # выключаем легенду, чтобы она не дублировала ось X
        )
        axes[1].set_title('Коэффициент корреляции по тикерам')
        axes[1].set_xlabel('Тикер')
        axes[1].set_ylabel('Коэффициент корреляции (r)')
        axes[1].tick_params(axis='x', rotation=45)
    else:
        axes[1].text(0.5, 0.5, 'Недостаточно данных', ha='center', va='center')

    plt.tight_layout()
    plt.savefig('data/correlation_analysis.png', dpi=300)
    print("Графики сохранены в data/correlation_analysis.png")
    
    # plt.show() # Раскомментируй, если хочешь, чтобы окно открывалось само