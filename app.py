import os
import json
import urllib.request
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from google.oauth2 import service_account
from googleapiclient.discovery import build as _build

app = Flask(__name__)

SP       = ZoneInfo('America/Sao_Paulo')
FORMATOS = ['%d/%m/%Y %H:%M', '%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M:%S']

def _parse_dt(s):
    for fmt in FORMATOS:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=SP)
        except ValueError:
            continue
    return None

def _sheets_service():
    raw  = os.environ.get('GSHEETS_SERVICE_ACCOUNT_JSON', '').strip().lstrip('=')
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return _build('sheets', 'v4', credentials=creds, cache_discovery=False)

@app.route('/run')
def run():
    token    = request.args.get('token', '').strip()
    expected = os.environ.get('SCHEDULER_TOKEN', '').strip().lstrip('=')
    if not expected or token != expected:
        return jsonify({'error': 'unauthorized'}), 401

    sheet_id = os.environ.get('GSHEETS_SPREADSHEET_ID', '').strip().lstrip('=')
    pa_key   = os.environ.get('PUSHALERT_API_KEY', '').strip().lstrip('=')

    svc    = _sheets_service()
    sheets = svc.spreadsheets()
    result = sheets.values().get(spreadsheetId=sheet_id, range='PushAlert Envios!A2:H').execute()
    rows   = result.get('values', [])

    agora    = datetime.now(SP)
    enviados, erros = [], []

    for i, row in enumerate(rows):
        while len(row) < 8:
            row.append('')
        titulo, mensagem, url, icone, publico, data_hora, status, _ = row

        if status.strip().lower() != 'pendente':
            continue

        dt = _parse_dt(data_hora.strip())
        if dt is None:
            sheets.values().update(spreadsheetId=sheet_id,
                range=f'PushAlert Envios!G{i+2}',
                valueInputOption='RAW',
                body={'values': [['erro: data inválida']]}).execute()
            erros.append({'linha': i+2, 'erro': 'data inválida'})
            continue

        if (dt - agora).total_seconds() > 300:
            continue

        try:
            pa_data = (
                f'title={urllib.parse.quote(titulo[:64])}'
                f'&message={urllib.parse.quote(mensagem[:192])}'
                f'&url={urllib.parse.quote(url)}'
            )
            if icone.strip():
                pa_data += f'&icon={urllib.parse.quote(icone.strip())}'

            if publico.strip().startswith('seg:'):
                seg_id = publico.strip().split(':')[1].strip()
                pa_url = f'https://api.pushalert.co/rest/v1/segment/{seg_id}/send'
            else:
                pa_url = 'https://api.pushalert.co/rest/v1/send'

            req = urllib.request.Request(
                pa_url, data=pa_data.encode(), method='POST',
                headers={'Authorization': f'api_key={pa_key}'}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_data = json.loads(resp.read())

            notif_id = str(resp_data.get('id', ''))
            sheets.values().update(spreadsheetId=sheet_id,
                range=f'PushAlert Envios!G{i+2}:H{i+2}',
                valueInputOption='RAW',
                body={'values': [['enviado', notif_id]]}).execute()
            enviados.append({'linha': i+2, 'titulo': titulo, 'id': notif_id})

        except Exception as e:
            sheets.values().update(spreadsheetId=sheet_id,
                range=f'PushAlert Envios!G{i+2}',
                valueInputOption='RAW',
                body={'values': [[f'erro: {str(e)[:80]}']]}).execute()
            erros.append({'linha': i+2, 'erro': str(e)})

    return jsonify({'ok': True, 'enviados': enviados, 'erros': erros})

@app.route('/')
def index():
    return jsonify({'status': 'push-scheduler online'})

if __name__ == '__main__':
    app.run(debug=True)
