name: oliveyoung_brand_ranking

on:
  schedule:
    # GitHub Actions는 UTC 기준. 한국시간 23:06 = UTC 14:06
    - cron: "6 14 * * *"
  workflow_dispatch:

jobs:
  run:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.10"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install playwright==1.45.0 openpyxl requests \
            google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib
          python -m playwright install --with-deps chromium

      - name: Run brand crawler via Web Unblocker proxy
        env:
          # 필수: WUB 프록시 (예) http://customer-USER-cc-KR-sessid-oy123:PASSWORD@unblock.oxylabs.io:60000
          PROXY_SERVER:        ${{ secrets.PROXY_SERVER }}

          # 선택: 슬랙/드라이브
          SLACK_WEBHOOK_URL:   ${{ secrets.SLACK_WEBHOOK_URL }}
          GDRIVE_FOLDER_ID:    ${{ secrets.GDRIVE_FOLDER_ID }}
          GOOGLE_CLIENT_ID:    ${{ secrets.GOOGLE_CLIENT_ID }}
          GOOGLE_CLIENT_SECRET: ${{ secrets.GOOGLE_CLIENT_SECRET }}
          GOOGLE_REFRESH_TOKEN: ${{ secrets.GOOGLE_REFRESH_TOKEN }}

          # (옵션) cf_clearance 쿠키가 있다면 주입
          CF_CLEARANCE:        ${{ secrets.CF_CLEARANCE }}
        run: |
          mkdir -p data
          python app_brand.py

      - name: Upload xlsx artifact
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: oliveyoung_brand_xlsx
          path: data/올리브영_브랜드_순위.xlsx
          if-no-files-found: warn

      - name: Upload debug artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: brand_debug
          path: |
            data/brand_debug.html
            data/brand_debug.png
            data/brand_debug.json
          if-no-files-found: ignore
