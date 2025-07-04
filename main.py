#!/usr/bin/env python
"""Команды для загрузки с YouTube в S3"""
import os
import sys
import tempfile
import argparse
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from decouple import config
import yt_dlp

def get_s3_client():
    """Создать клиент S3"""
    return boto3.client(
        's3',
        aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'),
        region_name=config('AWS_S3_REGION_NAME', default='us-east-1'),
        endpoint_url=config('AWS_S3_ENDPOINT_URL', default=None)
    )

def download_video(url: str, output_path: Path) -> tuple[Optional[Path], Optional[str]]:
    """Скачать видео с YouTube"""
    ydl_opts = {
        'outtmpl': str(output_path / '%(title)s.%(ext)s'),
        'format': 'best[height<=720]',
        'noplaylist': True,
    }
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            title = info.get('title', 'video')
            
            print(f"📹 Найдено: {title}")
            print(f"🔽 Скачиваем...")
            
            ydl.download([url])
            
            # Найти скачанный файл
            for file in output_path.glob("*"):
                if file.is_file():
                    print(f"✅ Скачано: {file.name}")
                    return file, title
            
            return None, None
            
    except Exception as e:
        print(f"❌ Ошибка скачивания: {e}")
        return None, None

def upload_file_to_s3(file_path: Path, s3_key: str) -> bool:
    """Загрузить файл в S3"""
    s3_client = get_s3_client()
    bucket = config('AWS_STORAGE_BUCKET_NAME')
    
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"✅ Бакет найден: {bucket}")
    except ClientError:
        print(f"❌ Бакет не найден: {bucket}")
        return False
    
    try:
        size_mb = os.path.getsize(file_path) / (1024*1024)
        print(f"📤 Загружаем {s3_key} ({size_mb:.1f} МБ)...")
        
        s3_client.upload_file(
            str(file_path),
            bucket,
            s3_key,
            ExtraArgs={'ACL': 'public-read'}
        )
        
        endpoint = config('AWS_S3_ENDPOINT_URL', default=None)
        if endpoint:
            url = f"{endpoint}/{bucket}/{s3_key}"
        else:
            url = f"https://{bucket}.s3.amazonaws.com/{s3_key}"
        
        print(f"✅ Загружено: {url}")
        return True
        
    except ClientError as e:
        print(f"❌ Ошибка загрузки: {e}")
        return False

def download():
    """
    Скачать видео с YouTube в локальную временную папку
    Использование: uv run download "https://youtube.com/watch?v=..."
    """
    if len(sys.argv) < 2:
        print("❌ Использование: uv run download <youtube_url>")
        sys.exit(1)
    
    url = sys.argv[1]
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        print(f"🚀 Скачиваем с: {url}")
        
        file_path, title = download_video(url, temp_path)
        if file_path:
            print(f"✅ Скачано в: {file_path}")
        else:
            print("❌ Скачивание не удалось")
            sys.exit(1)

def upload():
    """
    Загрузить локальный файл в S3
    Использование: uv run upload <путь_к_файлу> [--folder имя_папки]
    """
    parser = argparse.ArgumentParser(description='Загрузить файл в S3')
    parser.add_argument('file_path', help='Путь к локальному файлу')
    parser.add_argument('--folder', default='youtube.data', help='Папка в S3')
    
    args = parser.parse_args()
    
    file_path = Path(args.file_path)
    if not file_path.exists():
        print(f"❌ Файл не найден: {file_path}")
        sys.exit(1)
    
    s3_key = f"{args.folder}/{file_path.name}"
    
    print(f"🚀 Загружаем: {file_path}")
    success = upload_file_to_s3(file_path, s3_key)
    
    if not success:
        sys.exit(1)

def youtube_to_s3():
    """
    Скачать видео с YouTube и загрузить в S3
    Использование: uv run youtube-s3 "https://youtube.com/watch?v=..." [--folder имя_папки]
    """
    parser = argparse.ArgumentParser(description='YouTube в S3')
    parser.add_argument('url', help='URL YouTube')
    parser.add_argument('--folder', default='youtube', help='Папка в S3')
    
    args = parser.parse_args()
    
    print(f"🚀 Обрабатываем: {args.url}")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Скачиваем
        file_path, title = download_video(args.url, temp_path)
        if not file_path:
            print("❌ Скачивание не удалось")
            sys.exit(1)
        
        # Загружаем
        s3_key = f"{args.folder}/{file_path.name}"
        success = upload_file_to_s3(file_path, s3_key)
        
        if success:
            print("✅ Готово!")
        else:
            print("❌ Загрузка не удалась!")
            sys.exit(1)

def list_bucket():
    """
    Показать файлы в S3 бакете
    Использование: uv run list-bucket [--folder имя_папки]
    """
    parser = argparse.ArgumentParser(description='Показать содержимое S3 бакета')
    parser.add_argument('--folder', help='Фильтр по папке')
    
    args = parser.parse_args()
    
    s3_client = get_s3_client()
    bucket = config('AWS_STORAGE_BUCKET_NAME')
    
    try:
        if args.folder:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{args.folder}/")
        else:
            response = s3_client.list_objects_v2(Bucket=bucket)
        
        if 'Contents' in response:
            print(f"📁 Файлы в {bucket}:")
            for obj in response['Contents']:
                size_mb = obj['Size'] / (1024*1024)
                print(f"  📄 {obj['Key']} ({size_mb:.1f} МБ)")
        else:
            print("📁 Бакет пуст")
        
    except ClientError as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)

def check_config():
    """
    Проверить конфигурацию AWS
    Использование: uv run check-config
    """
    print("🔍 Проверяем конфигурацию...")
    
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY', 
        'AWS_STORAGE_BUCKET_NAME'
    ]
    
    missing = []
    for var in required_vars:
        try:
            value = config(var)
            print(f"✅ {var}: {'*' * (len(value) - 4) + value[-4:]}")
        except:
            missing.append(var)
            print(f"❌ {var}: Не задано")
    
    if missing:
        print(f"\n❌ Отсутствуют переменные: {', '.join(missing)}")
        print("Создайте файл .env с необходимыми переменными")
        sys.exit(1)
    else:
        print("\n✅ Конфигурация в порядке")
