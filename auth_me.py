# auth_me.py
# скрипт нужен для регистрации в тг (используется один раз)

import os
from telethon.sync import TelegramClient
from dotenv import load_dotenv

# Загружаем ключи из .env
load_dotenv()

api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

# СЮДА ПИШЕМ НОМЕР
phone = '+7XXXXXXXXXX' 
print(f"Запуск ручной авторизации для номера: {phone}")

# Используем синхронный клиент для обхода проблем с терминалом
client = TelegramClient('session_name', int(api_id), api_hash)

def manual_auth():
    client.connect()
    if not client.is_user_authorized():
        print("Запрашиваем код подтверждения...")
        client.send_code_request(phone)
        
        code = input('Введи код, который пришел в Telegram: ')
        
        try:
            client.sign_in(phone, code)
        except Exception as e:
            # Если стоит облачный пароль (2FA)
            if 'password' in str(e).lower():
                pwd = input('Введи облачный пароль (2FA): ')
                client.sign_in(password=pwd)
            else:
                raise e
                
    print("Файл 'session_name.session' создан. Теперь можно запускать main.py")
    client.disconnect()

if __name__ == '__main__':
    manual_auth()