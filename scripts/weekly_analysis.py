import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import requests
import json

# Google Drive 설정
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_gdrive_service():
    """Google Drive 서비스 인스턴스 생성"""
    # 환경 변수에서 서비스 계정 정보 가져오기
    client_email = os.environ.get('GOOGLE_CLIENT_EMAIL')
    private_key = os.environ.get('GOOGLE_PRIVATE_KEY', '').replace('\\n', '\n')
    
    creds = service_account.Credentials.from_service_account_info(
        {
            "type": "service_account",
            "client_email": client_email,
            "private_key": private_key,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

def download_csv_files(service, folder_id):
    """Google Drive에서 CSV 파일 다운로드"""
    # 7일 전 날짜 계산
    start_date = (datetime.now() - timedelta(days=7)).isoformat() + 'Z'
    
    # 쿼리 문자열 수정 (EOL 오류 해결)
    query = f"'{folder_id}' in parents and mimeType='text/csv' and createdTime > '{start_date}'"
    
    results = service.files().list(
        q=query,
        pageSize=100, 
        fields="files(id, name, createdTime)"
    ).execute()
    
    files = results.get('files', [])
    
    downloaded_files = {}
    for file in files:
        request = service.files().get_media(fileId=file['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        content = fh.read().decode('utf-8')
        
        # 파일 이름에서 플랫폼 식별
        if '올리브영_랭킹' in file['name']:
            platform = 'oliveyoung_kr'
        elif '올리브영글로벌_랭킹' in file['name']:
            platform = 'oliveyoung_global'
        elif '아마존US_뷰티_랭킹' in file['name']:
            platform = 'amazon_us'
        elif '큐텐재팬_뷰티_랭킹' in file['name']:
            platform = 'qoo10_jp'
        elif '다이소몰_뷰티위생_일간' in file['name']:
            platform = 'daiso'
        else:
            continue
        
        downloaded_files[platform] = {
            'name': file['name'],
            'content': content,
            'date': file['createdTime'][:10]  # YYYY-MM-DD 형식
        }
    
    return downloaded_files

# 나머지 함수들은 동일하게 유지 (extract_product_key, analyze_weekly_data, generate_markdown_report, send_slack_notification)

def main():
    try:
        # Google Drive 서비스 초기화
        service = get_gdrive_service()
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')
        
        if not folder_id:
            print("GDRIVE_FOLDER_ID 환경 변수가 설정되지 않았습니다.")
            return
        
        # CSV 파일 다운로드
        files_data = download_csv_files(service, folder_id)
        
        if not files_data:
            print("분석할 데이터가 없습니다.")
            return
        
        # 데이터 분석
        analysis_results = analyze_weekly_data(files_data)
        
        # 보고서 생성
        report_date = datetime.now().strftime('%Y-%m-%d')
        report = generate_markdown_report(analysis_results, report_date)
        
        # 보고서 저장
        os.makedirs('reports', exist_ok=True)
        report_filename = f'reports/weekly_report_{report_date}.md'
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"보고서가 생성되었습니다: {report_filename}")
        
        # 슬랙 알림 전송
        slack_webhook = os.environ.get('SLACK_WEBHOOK_URL')
        if slack_webhook:
            slack_message = f"*주간 뷰티 랭킹 분석 완료* ({report_date})\n"
            for platform, results in analysis_results.items():
                if 'error' not in results and results.get('top10'):
                    top_product = results['top10'][0]
                    brand = top_product.get('brand', 'N/A')
                    name = top_product.get('name', top_product.get('product_name', 'N/A'))
                    slack_message += f"{platform.upper()} 1위: {brand} - {name}\n"
            
            send_slack_notification(slack_message, slack_webhook)
        else:
            print("SLACK_WEBHOOK_URL이 설정되지 않아 슬랙 알림을 전송하지 않습니다.")
        
    except Exception as e:
        error_msg = f"주간 분석 중 오류 발생: {str(e)}"
        print(error_msg)
        
        # 슬랙으로 오류 알림 전송
        slack_webhook = os.environ.get('SLACK_WEBHOOK_URL')
        if slack_webhook:
            send_slack_notification(error_msg, slack_webhook)

if __name__ == "__main__":
    main()
