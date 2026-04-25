name: Scrape Harris County Leads
on:
  schedule:
    - cron: "0 7 * * *"
  workflow_dispatch:

permissions:
  contents: write
  pages: write
  id-token: write

jobs:
  scrape:
    runs-on: ubuntu-22.04
    timeout-minutes: 90
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
          cache-dependency-path: scraper/requirements.txt

      - name: Install Python dependencies
        run: pip install -r scraper/requirements.txt

      - name: Install Playwright + Chromium
        run: python -m playwright install --with-deps chromium

      - name: Cache HCAD parcel data
        id: hcad-cache
        uses: actions/cache@v4
        with:
          path: data/Real_acct_owner.zip
          key: hcad-week-${{ github.run_id }}
          restore-keys: |
            hcad-week-

      - name: Download HCAD parcel data from Google Drive
        run: |
          mkdir -p data
          if [ -f data/Real_acct_owner.zip ]; then
            SIZE=$(stat -c%s data/Real_acct_owner.zip)
            echo "Cached file size: $SIZE bytes"
            if [ "$SIZE" -gt 50000000 ]; then
              echo "Cache valid, skipping download"
              exit 0
            else
              echo "Cache too small, re-downloading..."
              rm data/Real_acct_owner.zip
            fi
          fi
          FILE_ID="1edpPMYI5rzx6nCGH5x8tGdo3JuyluNR8"
          echo "Downloading from Google Drive..."
          pip install gdown -q
          gdown "https://drive.google.com/uc?id=${FILE_ID}" -O data/Real_acct_owner.zip
          SIZE=$(stat -c%s data/Real_acct_owner.zip)
          echo "Downloaded size: $SIZE bytes"
          if [ "$SIZE" -lt 50000000 ]; then
            echo "ERROR: File too small - download failed"
            exit 1
          fi
          echo "Done: $(du -sh data/Real_acct_owner.zip)"

      - name: Verify HCAD file
        run: |
          ls -lh data/Real_acct_owner.zip
          python3 -c "
          import zipfile
          z = zipfile.ZipFile('data/Real_acct_owner.zip')
          print('Files in zip:', z.namelist())
          "

      - name: Run scraper
        env:
          LOOKBACK_DAYS: "7"
          HEADLESS: "true"
        run: python scraper/fetch.py

      - name: Commit updated records
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add dashboard/records.json data/records.json data/ghl_export.csv || true
          git diff --cached --quiet && echo "No changes to commit" || \
            git commit -m "chore: update leads $(date -u +%Y-%m-%d)"
          git pull --rebase && git push

  deploy-pages:
    needs: scrape
    runs-on: ubuntu-22.04
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          ref: main

      - name: Setup Pages
        uses: actions/configure-pages@v4

      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: dashboard/

      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
