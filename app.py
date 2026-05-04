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

DIAS_PT = {
    0: 'Segunda',
    1: 'Terça',
    2: 'Quarta',
    3: 'Quinta',
    4: 'Sexta',
    5: 'Sábado',
    6: 'Domingo',
}

def _aba_hoje():
    return DIAS_PT[datetime.now(SP).weekday()]

def _parse_dt(s):
    for fmt in FORMATOS:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=SP)
        except ValueError:
            continue
    return None

def _parse_horario(s):
    """Retorna datetime de hoje com o horário HH:MM se o formato for apenas HH:MM."""
    try:
        t = datetime.strptime(s.strip(), '%H:%M')
        agora = datetime.now(SP)
        return agora.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
    except ValueError:
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

    aba    = _aba_hoje()
    svc    = _sheets_service()
    sheets = svc.spreadsheets()

    try:
        result = sheets.values().get(
            spreadsheetId=sheet_id,
            range=f'{aba}!A2:H1000'
        ).execute()
    except Exception as e:
        return jsonify({'error': f'Aba "{aba}" não encontrada: {str(e)}'}), 400

    rows  = result.get('values', [])
    agora = datetime.now(SP)
    hoje  = agora.strftime('%Y-%m-%d')
    enviados, erros = [], []

    for i, row in enumerate(rows):
        while len(row) < 8:
            row.append('')
        titulo, mensagem, url, icone, publico, data_hora, status, ultimo_envio = row

        if status.strip().lower() != 'pendente':
            continue

        # Detecta se é recorrente (só HH:MM) ou pontual (data completa)
        recorrente = False
        dt = _parse_dt(data_hora.strip())
        if dt is None:
            dt = _parse_horario(data_hora.strip())
            if dt is None:
                sheets.values().update(spreadsheetId=sheet_id,
                    range=f'{aba}!G{i+2}',
                    valueInputOption='RAW',
                    body={'values': [['erro: data inválida']]}).execute()
                erros.append({'linha': i+2, 'erro': 'data inválida'})
                continue
            recorrente = True

        # Para recorrentes, pula se já foi enviado hoje
        if recorrente and ultimo_envio.strip() == hoje:
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
            elif publico.strip().startswith('aud:'):
                aud_id = publico.strip().split(':')[1].strip()
                pa_url = f'https://api.pushalert.co/rest/v1/audience/{aud_id}/send'
            else:
                pa_url = 'https://api.pushalert.co/rest/v1/send'

            req = urllib.request.Request(
                pa_url, data=pa_data.encode(), method='POST',
                headers={'Authorization': f'api_key={pa_key}'}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp_data = json.loads(resp.read())

            notif_id = str(resp_data.get('id', ''))

            if recorrente:
                # Mantém pendente, registra data do envio em H
                sheets.values().update(spreadsheetId=sheet_id,
                    range=f'{aba}!H{i+2}',
                    valueInputOption='RAW',
                    body={'values': [[hoje]]}).execute()
            else:
                # Pontual: marca como enviado e salva ID
                sheets.values().update(spreadsheetId=sheet_id,
                    range=f'{aba}!G{i+2}:H{i+2}',
                    valueInputOption='RAW',
                    body={'values': [['enviado', notif_id]]}).execute()

            enviados.append({'linha': i+2, 'titulo': titulo, 'id': notif_id, 'recorrente': recorrente})

        except Exception as e:
            sheets.values().update(spreadsheetId=sheet_id,
                range=f'{aba}!G{i+2}',
                valueInputOption='RAW',
                body={'values': [[f'erro: {str(e)[:80]}']]}).execute()
            erros.append({'linha': i+2, 'erro': str(e)})

    return jsonify({'ok': True, 'aba': aba, 'enviados': enviados, 'erros': erros})

@app.route('/debug')
def debug():
    token    = request.args.get('token', '').strip()
    expected = os.environ.get('SCHEDULER_TOKEN', '').strip().lstrip('=')
    if not expected or token != expected:
        return jsonify({'error': 'unauthorized'}), 401
    sheet_id = os.environ.get('GSHEETS_SPREADSHEET_ID', '').strip().lstrip('=')
    try:
        svc  = _sheets_service()
        meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
        abas = [s['properties']['title'] for s in meta.get('sheets', [])]
        return jsonify({'spreadsheet': meta.get('properties', {}).get('title'), 'abas': abas, 'aba_hoje': _aba_hoje()})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/')
def index():
    return jsonify({'status': 'push-scheduler online', 'aba_hoje': _aba_hoje()})

if __name__ == '__main__':
    app.run(debug=True)
