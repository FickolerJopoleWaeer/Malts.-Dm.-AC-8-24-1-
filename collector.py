# collector.py
# Скрипт для работы с Telegram

import os
import re
import asyncio
import csv
import pandas as pd
from telethon.sync import TelegramClient
from dotenv import load_dotenv
import emoji                    # для очистки от смайлов
from datetime import timedelta  # Для сдвига времени UTC +3 до МСК
# Загружаем переменные из .env
load_dotenv()

# Очистка
def clean_news_text(text):
    if not text:
        return ""
    text = text.replace('\n', ' ')                  # Заменяем переносы на пробелы
    text = re.sub(r'\[.*?\]', '', text)             # Убираем всё в [квадратных скобках]
    text = re.sub(r'\(.*?\)', '', text)             # Убираем всё в (круглых скобках)
    text = re.sub(r'https?://\S+', '', text)        # Удаляем ссылки
    text = text.replace('*', '')                    # Удаляем звездочки
    text = re.sub(r'["\'«»“”„‟‘’‛′″′]', '', text)   # Удаляем кавычки
    text = emoji.replace_emoji(text, replace='')    # Удаляем эмодзи
    if "Читать далее" in text:                      # Обрезаем рекламный хвост 
        text = text.replace('Читать далее', '')
    
    text = re.sub(r'\s+', ' ', text).strip()        # Убираем двойные пробелы
    return text

async def collect_data(channel_username, limit=10000):
    file_path = 'data/raw_posts.csv'
    
    if os.path.exists(file_path):
        print(f"Данные найдены локально.")
        return pd.read_csv(file_path)

    print(f"Файл не найден. Начинаем сбор постов из @{channel_username}...")

    api_id = os.getenv('TG_API_ID')
    api_hash = os.getenv('TG_API_HASH')

    if not api_id or not api_hash:
        raise ValueError("Ошибка: API_ID или API_HASH не найдены в .env файле")

    if not os.path.exists('data'):
        os.makedirs('data')

    client = TelegramClient('session_name', int(api_id), api_hash)
    
    try:
        await client.connect()
        
        if not await client.is_user_authorized():
            print("Ошибка: Сессия не авторизована. Запусти auth_me.py")
            return pd.DataFrame()

        # ... (код подключения к клиенту) ...
        posts = []
        fetched = 0
        chunk_size = 200 # Собираем кусками по 200
        try:

            print(f"Скачиваем {limit} постов и фильтруем сообщения, это займет пару секунд...")
            
            # Для отслеживания прогресса
            last_id = 0 
            #k=0

            while fetched < limit:
                current_limit = min(chunk_size, limit - fetched)

                # offset_id помогает забирать сообщения, которые идут СЛЕДУЮЩИМИ
                async for message in client.iter_messages(channel_username, limit=current_limit, offset_id=last_id):
                    #k+=1
                    if message.text:
                        raw_text = message.text
                        # Переводим время из UTC в MSK (UTC+3)
                        msk_time = message.date + timedelta(hours=3)

                        # 1. Пропускаем посты БЕЗ хештегов
                        if '#' not in raw_text:     
                            continue
                        
                        # 2. Пропускаем огромные посты или солянки (больше 3 хештегов)
                        hashtags_count = len(re.findall(r'#([A-Z]+)', raw_text))
                        if len(raw_text) > 1000 or hashtags_count > 3:
                            continue

                        # 3. Очищаем текст
                        cleaned = clean_news_text(raw_text)     
                        
                        if cleaned:
                            posts.append({
                                'date': msk_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'text': cleaned
                            })
                        
                        #if k%500 == 0:
                            #print(f"Просмотрено {k} сообщений, отобрано {len(posts)} постов с упоминаниями...")
                last_id = message.id
                fetched += current_limit
                print(f"--- Собрано: {len(posts)} подходящих постов (просмотрено {fetched}) ---")

                # Даем системе "продышаться" 2 секунды между кусками
                await asyncio.sleep(2)

        except asyncio.CancelledError:
            print("--- Сбор данных был прерван системой (CancelledError) ---")
            # Если уже что-то успели собрать — вернем хотя бы это
            if posts:
                print(f"Возвращаем {len(posts)} постов, собранных до обрыва.")
                return pd.DataFrame(posts)
            raise # Если пусто — пробрасываем ошибку дальше

        if not posts:
            print("После фильтрации не осталось ни одного подходящего поста.")
            return pd.DataFrame()

        df = pd.DataFrame(posts)
        df.to_csv(
            file_path, 
            index=False, 
            encoding='utf-8-sig', # 'utf-8-sig' для Excel (лучше понимает кодировку)
            quoting=csv.QUOTE_ALL # оборачиваем текст в кавычки везде
        )
        print(f"Сохранено {len(df)} идеально чистых постов (из {limit} просмотренных)")
        return df

    except Exception as e:
        print(f"Произошла ошибка при сборе: {e}")
        return pd.DataFrame()
    
    finally:
        await client.disconnect()