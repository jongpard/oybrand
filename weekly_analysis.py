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
SERVICE_ACCOUNT_FILE = 'credentials.json'

def get_gdrive_service():
    """Google Drive 서비스 인스턴스 생성"""
    creds = service_account.Credentials.from_service_account_info(
        {
            "type": "service_account",
            "client_email": os.environ.get('GOOGLE_CLIENT_EMAIL'),
            "private_key": os.environ.get('GOOGLE_PRIVATE_KEY').replace('\\n', '\n'),
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

def download_csv_files(service, folder_id):
    """Google Drive에서 CSV 파일 다운로드"""
    query = f"'{folder_id}' in parents and mimeType='text/csv' and createdTime > '{(
        datetime.now() - timedelta(days=7)).isoformat()}'"
    
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

def extract_product_key(url, platform):
    """URL에서 제품 키 추출"""
    if platform == 'oliveyoung_kr':
        match = re.search(r'goodsNo=([A-Z0-9]+)', url)
        return match.group(1) if match else None
    elif platform == 'oliveyoung_global':
        match = re.search(r'prdtNo=([A-Z0-9]+)', url)
        return match.group(1) if match else None
    elif platform == 'amazon_us':
        match = re.search(r'dp/([A-Z0-9]{10})', url)
        return match.group(1) if match else None
    elif platform == 'qoo10_jp':
        # product_code 컬럼이 있는 경우
        return None  # 별도 처리 필요
    elif platform == 'daiso':
        match = re.search(r'pdNo=(\d+)', url)
        return match.group(1) if match else None
    return None

def analyze_weekly_data(files_data):
    """주간 데이터 분석"""
    analysis_results = {}
    
    for platform, data in files_data.items():
        try:
            # CSV 데이터를 DataFrame으로 변환
            df = pd.read_csv(io.StringIO(data['content']))
            
            # 플랫폼별 키 추출
            if 'url' in df.columns:
                df['product_key'] = df['url'].apply(lambda x: extract_product_key(x, platform))
            
            # 분석 결과 저장
            analysis_results[platform] = {
                'top10': df.head(10).to_dict('records'),
                'new_entries': [],  # 이전 주와 비교한 신규 진입 제품
                'biggest_climbers': [],  # 가장 많이 순위 상승한 제품
                'biggest_fallers': [],  # 가장 많이 순위 하락한 제품
                'total_products': len(df)
            }
            
        except Exception as e:
            print(f"Error processing {platform}: {str(e)}")
            analysis_results[platform] = {'error': str(e)}
    
    return analysis_results

def generate_markdown_report(analysis_results, report_date):
    """마크다운 보고서 생성"""
    report = f"# 주간 뷰티 랭킹 분석 리포트 ({report_date})\n\n"
    
    for platform, results in analysis_results.items():
        if 'error' in results:
            report += f"## {platform.upper()} 분석 중 오류 발생: {results['error']}\n\n"
            continue
            
        report += f"## {platform.upper()} Top 10\n\n"
        
        # Top 10 테이블
        report += "| 순위 | 브랜드 | 제품명 | 가격 |\n"
        report += "|------|--------|--------|------|\n"
        
        for product in results['top10']:
            brand = product.get('brand', 'N/A')
            name = product.get('name', product.get('product_name', 'N/A'))
            price = product.get('sale_price', product.get('price', 'N/A'))
            
            report += f"| {product.get('rank', 'N/A')} | {brand} | {name} | {price} |\n"
        
        report += "\n"
    
    return report

def send_slack_notification(message, webhook_url):
    """슬랙으로 결과 전송"""
    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되지 않았습니다.")
        return
    
    payload = {
        "text": message,
        "username": "Beauty Ranking Bot",
        "icon_emoji": ":chart_with_upwards_trend:"
    }
    
    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            print(f"Slack 전송 실패: {response.status_code}")
    except Exception as e:
        print(f"Slack 전송 중 오류: {str(e)}")

def main():
    # Google Drive 서비스 초기화
    try:
        service = get_gdrive_service()
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')
        
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
        with open(f'reports/weekly_report_{report_date}.md', 'w', encoding='utf-8') as f:
            f.write(report)
        
        # 슬랙 알림 전송 (간략한 버전)
        slack_message = f"*주간 뷰티 랭킹 분석 완료* ({report_date})\n"
        for platform, results in analysis_results.items():
            if 'error' not in results:
                top_product = results['top10'][0] if results['top10'] else {}
                brand = top_product.get('brand', 'N/A')
                name = top_product.get('name', top_product.get('product_name', 'N/A'))
                slack_message += f"{platform.upper()} 1위: {brand} - {name}\n"
        
        send_slack_notification(slack_message, os.environ.get('SLACK_WEBHOOK_URL'))
        
    except Exception as e:
        error_msg = f"주간 분석 중 오류 발생: {str(e)}"
        print(error_msg)
        send_slack_notification(error_msg, os.environ.get('SLACK_WEBHOOK_URL'))

if __name__ == "__main__":
    main()
