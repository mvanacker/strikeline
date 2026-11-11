#!/usr/bin/env python3
"""Build the static demo: demo-dist/ holds index.html (the real page with demo.js injected) and
demo.js. Upload that folder to any static host, e.g. Cloudflare Pages. No server is involved:
demo.js replaces the /api endpoints and the price stream with in-browser simulations."""
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'demo-dist'

html = (ROOT / 'static' / 'index.html').read_text(encoding='utf-8')
if '<head>' not in html:
    raise SystemExit('static/index.html has no <head> tag to inject into')
html = html.replace('<head>', '<head>\n<script src="demo.js"></script>', 1)

# The demo has no Coinbase or Deribit feed, so do not claim one.
REPLACEMENTS = [
    ('Live · Coinbase', 'Live · simulated'),
    ('Connecting to Coinbase…', 'Starting simulated feed…'),
    ('Line is the Coinbase bid/ask mid.', 'Line is the simulated mid price.'),
    ('on live BTC, ETH and XRP prices', 'on simulated BTC, ETH and XRP prices'),
    ("Prices are live BTC-USD, ETH-USD and XRP-USD mids from Coinbase's public feed;",
     "In this demo, prices are simulated random walks for BTC-USD, ETH-USD and XRP-USD (the full app reads Coinbase's public feed);"),
]
for old, new in REPLACEMENTS:
    if old not in html:
        raise SystemExit(f'expected text not found in static/index.html: {old!r}')
    html = html.replace(old, new)

OUT.mkdir(exist_ok=True)
(OUT / 'index.html').write_text(html, encoding='utf-8')
shutil.copyfile(ROOT / 'demo' / 'demo.js', OUT / 'demo.js')
print(f'Wrote {OUT}: index.html, demo.js')
