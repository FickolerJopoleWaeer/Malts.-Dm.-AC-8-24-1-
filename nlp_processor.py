


# nlp_processor.py
# Для оценки тональности постов

import re
import pandas as pd

def identify_companies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Алгоритм сопоставления текстовых упоминаний с эмитентами (NER)
    """
    print("Запуск модуля оценки тональности")
    extracted_data = []

    for _, row in df.iterrows():
        text = str(row['text'])

        # 1. Поиск по явным хештегам из Smart-Lab (например, #SBER, #GAZP)
        # Ищем символ '#' за которым идут заглавные английские буквы
        hashtags = re.findall(r'#([A-Z]+)', text)
        found_tickers = set(hashtags)

        # 2. Если хештегов нет, ищем по нашему словарю-заглушке
        # словарь в Report.md
        '''
        if not found_tickers:
            text_lower = text.lower()
            for keyword, ticker in COMPANY_DICT.items():
                if keyword in text_lower:
                    found_tickers.add(ticker)
        '''
        # Если нашли хотя бы один тикер, сохраняем пост для этого тикера.
        # Если в новости 2 тикера (например, #GAZP #NVTK), создадутся 2 отдельные строки
        for ticker in found_tickers:
            extracted_data.append({'date': row['date'], 'ticker': ticker, 'text': text})

    new_df = pd.DataFrame(extracted_data)
    print(f"Из {len(df)} постов успешно выделено {len(new_df)} упоминаний компаний.")
    return new_df

def analyze_social_reaction(df: pd.DataFrame) -> pd.DataFrame:
    """
    Классификация текстов по признакам потенциальной реакции социума
    """
    print("Запуск моделирования социальной реакции")
    try:
        from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
        model_name = "cointegrated/rubert-tiny-sentiment-balanced"
        # https://huggingface.co/cointegrated/rubert-tiny-sentiment-balanced
        # Явная загрузка токенизатора и модел
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModelForSequenceClassification.from_pretrained(model_name)
        
        # Важно: добавляем top_k=None, чтобы получать все вероятности
        sentiment_pipeline = pipeline(
            "sentiment-analysis", 
            model=model, 
            tokenizer=tokenizer,
            top_k=None 
        )
        
        
        def get_neural_score(text):
            # BERT принимает тексты до ~512 токенов, обрезаем строку для надежности
            # Получаем список словарей [{'label': 'positive', 'score': 0.9}, ...]
            results = sentiment_pipeline(text, truncation=True, max_length=512)[0]
            
            # Превращаем в словарь для быстрого доступа
            scores = {res['label']: res['score'] for res in results}
            
            # Вычисляем итоговый балл как разницу позитива и негатива
            # Это даст нам плавную шкалу от -1.0 до 1.0
            pos = scores.get('positive', 0.0)
            neg = scores.get('negative', 0.0)

            # print(round(pos, 2), ' | ', round(neg, 2), ' -- ', text) 
            # ПРИМЕР: 0.08  |  0.07  --  ТНС энерго Ростов-на-Дону РСБУ 2025 г.: выручка ₽76,94 млрд чистая прибыль ₽3,01 млрд #RTSB

            return round(pos - neg, 4) # Округлим до 4 знаков

        df['sentiment_score'] = df['text'].apply(get_neural_score)
        print("Оценка успешно завершена (дробная шкала)")

    except Exception as e:
        # Если нейросеть не сработала (ошибка импорта, нет памяти и т.д.)
        print(f"\n[Ошибка] Не удалось использовать нейросеть: {e}")
        print("Используется алгоритм-заглушка...")
        
        def get_mock_score(text):
            t = text.lower()
            # Словари маркеров социальной реакции
            positive_triggers = ['вырос', 'рост', 'дивиденды', 'поддерживают', 'опережение']
            negative_triggers = ['упасть', 'кризис', 'задолженность', 'отказ', 'угроза', 'война']
            
            if any(w in t for w in positive_triggers): return 1.0
            if any(w in t for w in negative_triggers): return -1.0
            return 0.0
            
        df['sentiment_score'] = df['text'].apply(get_mock_score)
        print("Оценка завершена с использованием словаря-заглушки")

    return df