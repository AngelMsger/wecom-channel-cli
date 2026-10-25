#!/usr/bin/env python3
"""Check offline site assets, internal links/anchors and accidental external assets."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit
import json
root = Path(__file__).resolve().parents[1] / 'docs'
class Page(HTMLParser):
    def __init__(self):
        super().__init__(); self.links=[]; self.ids=set(); self.remote_assets=[]; self.title=False
    def handle_starttag(self, tag, attrs):
        a=dict(attrs)
        if 'id' in a: self.ids.add(a['id'])
        if tag=='title': self.title=True
        for attr in ('href','src'):
            if attr in a:
                self.links.append(a[attr])
                if tag in ('script','img','link') and urlsplit(a[attr]).scheme in ('https','http'): self.remote_assets.append(a[attr])
pages={}
for path in root.rglob('*.html'):
    p=Page();p.feed(path.read_text());pages[path]=p
issues=[]
for path,page in pages.items():
    if not page.title:issues.append(f'{path.name}: missing title')
    if page.remote_assets:issues.append(f'{path.name}: remote page assets {page.remote_assets}')
    for link in page.links:
        url=urlsplit(link)
        if url.scheme or url.netloc:continue
        target=(path.parent / unquote(url.path)).resolve() if url.path else path
        if target.is_dir():target=target/'index.html'
        if not target.is_relative_to(root.resolve()) or not target.exists():issues.append(f'{path.relative_to(root)}: missing {link}')
        elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].ids:issues.append(f'{path.relative_to(root)}: missing anchor {link}')
print(json.dumps({'pages':len(pages),'issues':issues},indent=2))
raise SystemExit(bool(issues))
