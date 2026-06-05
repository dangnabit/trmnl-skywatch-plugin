#!/usr/bin/env bash
set -euo pipefail

URL='https://davidmegginson.github.io/ourairports-data/airports.csv'
OUT='AIRPORTS.md'

command -v curl >/dev/null 2>&1 || { echo 'curl is required'; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo 'python3 is required'; exit 1; }

TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

curl -fsSL "$URL" -o "$TMPFILE"

python3 <<'PY'
import csv
import io
import sys
from datetime import date

input_path = sys.argv[1]
out_path = sys.argv[2]

with open(input_path, newline='', encoding='utf-8') as f:
    text = f.read()
reader = csv.DictReader(io.StringIO(text))

airports = []
for row in reader:
    if row.get('type') not in ('large_airport', 'medium_airport'):
        continue
    iata = (row.get('iata_code') or '').strip()
    if not iata:
        continue
    airports.append({
        'iata':    iata,
        'icao':    (row.get('gps_code') or row.get('ident') or '').strip(),
        'name':    (row.get('name') or '').strip(),
        'city':    (row.get('municipality') or '').strip(),
        'country': (row.get('iso_country') or '').strip(),
    })

airports.sort(key=lambda a: a['iata'])

lines = [
    '# SkyWatch Airport List',
    '',
    'Large and medium airports shown on the SkyWatch radar display. Updated daily from [OurAirports](https://ourairports.com). Last updated: {}.'.format(date.today().isoformat()),
    '',
    '**{} airports** across {} countries.'.format(len(airports), len({a['country'] for a in airports})),
    '',
    '| IATA | ICAO | Airport | City | Country |',
    '|------|------|---------|------|---------|',
]
for a in airports:
    lines.append(f"| {a['iata']} | {a['icao']} | {a['name']} | {a['city']} | {a['country']} |")
lines.append('')

with open(out_path, 'w', encoding='utf-8', newline='') as f:
    f.write('\n'.join(lines))

print(f'Written {len(airports)} airports to {out_path}')
PY
"$TMPFILE" "$OUT"
