from flask import Flask, render_template, request, redirect, url_for, Response, jsonify, session
from database import get_connection, init_db
import datetime
import calendar
import csv
import io
import json
import os
import hashlib
import functools

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
except ImportError:
    pass

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

APP_USERNAME = os.environ.get('APP_USERNAME', '')
APP_PASSWORD = os.environ.get('APP_PASSWORD', '')

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if (request.form.get('username') == APP_USERNAME and
                request.form.get('password') == APP_PASSWORD):
            session['logged_in'] = True
            next_url = request.args.get('next') or url_for('dashboard')
            return redirect(next_url)
        error = 'Credenziali non valide.'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

MESI_IT = ['', 'Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno',
           'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre']

CATEGORIE = [
    "contratti",
    "consulenza",
    "marketing",
    "personale",
    "collaboratori",
    "fornitori",
    "affitto",
    "software",
    "partner",
    "banche",
    "tasse",
    "altro",
]

CATEGORIE_COSTI = [
    "finanziamento",
    "leasing",
    "affitto",
    "software",
    "utenze",
    "assicurazione",
    "personale",
    "fornitori",
    "tasse",
    "marketing",
    "banca",
    "altro",
]

RICORRENZE = {
    "mensile": 1,
    "bimestrale": 2,
    "trimestrale": 3,
    "semestrale": 6,
    "annuale": 12,


}


def _add_months(d, months):
    """Aggiunge mesi a una data gestendo correttamente la fine del mese."""
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return datetime.date(year, month, day)


def _genera_rate_costo(conn, scadenza_costo_id, data_prima, ricorrenza, num_rate, importo_default, uscita_cassa_default=0):
    """Genera automaticamente le rate per una scadenza costo."""
    mesi = RICORRENZE.get(ricorrenza, 1)
    data = datetime.date.fromisoformat(data_prima)
    for i in range(num_rate):
        data_rata = _add_months(data, mesi * i)
        conn.execute("""
            INSERT INTO rate_scadenza_costo (scadenza_costo_id, numero_rata, importo, uscita_cassa, data_scadenza)
            VALUES (%s, %s, %s, %s, %s)
        """, (scadenza_costo_id, i + 1, importo_default, uscita_cassa_default, data_rata.isoformat()))


def _parse_csv_bancario(content_bytes):
    """
    Analizza un CSV bancario e restituisce una lista di dict:
    {data, descrizione, importo (float positivo), tipo ('entrata'/'uscita'), categoria (opzionale)}
    Formati supportati: Fineco, N26, Generico (data,descrizione,importo[,tipo][,categoria])
    """
    text = None
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1'):
        try:
            text = content_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("Impossibile decodificare il file. Assicurati che sia un CSV valido.")

    sample = '\n'.join(text.splitlines()[:10])
    delimiter = ';' if sample.count(';') >= sample.count(',') else ','

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    all_rows = [row for row in reader if any(cell.strip() for cell in row)]

    format_type = None
    header_idx = None
    header = None
    for i, row in enumerate(all_rows):
        rl = [c.strip().strip('"').lower() for c in row]
        if 'data operazione' in rl and ('entrate' in rl or 'uscite' in rl):
            format_type, header_idx, header = 'fineco', i, rl
            break
        if 'data operazione' in rl and ('debito' in rl or 'credito' in rl):
            format_type, header_idx, header = 'sella', i, rl
            break
        if 'date' in rl and 'payee' in rl and any('amount' in c for c in rl):
            format_type, header_idx, header = 'n26', i, rl
            break
        if 'data' in rl and 'descrizione' in rl and 'importo' in rl:
            format_type, header_idx, header = 'generico', i, rl
            break

    if format_type is None:
        raise ValueError(
            "Formato non riconosciuto. Formati supportati: Fineco, Banca Sella, N26, "
            "o generico con colonne: data, descrizione, importo [, tipo] [, categoria]"
        )

    data_rows = all_rows[header_idx + 1:]
    movimenti = []

    def parse_date(s):
        for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y'):
            try:
                return datetime.datetime.strptime(s.strip(), fmt).strftime('%Y-%m-%d')
            except ValueError:
                pass
        return None

    def parse_number(s):
        s = s.strip().strip('"')
        if not s or s in ('-', 'n/a', ''):
            return None
        if ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        elif ',' in s:
            s = s.replace(',', '.')
        try:
            return float(s)
        except ValueError:
            return None

    if format_type == 'sella':
        i_data = header.index('data operazione')
        i_desc = header.index('descrizione')
        i_deb  = header.index('debito')   if 'debito'  in header else -1
        i_cred = header.index('credito')  if 'credito' in header else -1
        i_cat  = header.index('categoria') if 'categoria' in header else -1
        i_cod  = header.index('codice identificativo') if 'codice identificativo' in header else -1
        for row in data_rows:
            if len(row) <= max(i_data, i_desc):
                continue
            data_iso = parse_date(row[i_data])
            if not data_iso:
                continue
            descrizione = row[i_desc].strip().strip('"')
            if not descrizione:
                continue
            deb  = parse_number(row[i_deb])  if i_deb  >= 0 and i_deb  < len(row) else None
            cred = parse_number(row[i_cred]) if i_cred >= 0 and i_cred < len(row) else None
            if cred and cred > 0:
                m = {'data': data_iso, 'descrizione': descrizione, 'importo': cred, 'tipo': 'entrata'}
            elif deb and deb < 0:
                m = {'data': data_iso, 'descrizione': descrizione, 'importo': abs(deb), 'tipo': 'uscita'}
            else:
                continue
            if i_cat >= 0 and i_cat < len(row) and row[i_cat].strip():
                m['categoria'] = row[i_cat].strip()
            if i_cod >= 0 and i_cod < len(row) and row[i_cod].strip():
                m['codice_banca'] = row[i_cod].strip().strip('"')
            movimenti.append(m)

    elif format_type == 'fineco':
        i_data = header.index('data operazione')
        i_desc = header.index('descrizione')
        i_entr = header.index('entrate') if 'entrate' in header else -1
        i_usc  = header.index('uscite')  if 'uscite'  in header else -1
        for row in data_rows:
            if len(row) <= max(i_data, i_desc):
                continue
            data_iso = parse_date(row[i_data])
            if not data_iso:
                continue
            descrizione = row[i_desc].strip().strip('"')
            if not descrizione:
                continue
            entr = parse_number(row[i_entr]) if i_entr >= 0 and i_entr < len(row) else None
            usc  = parse_number(row[i_usc])  if i_usc  >= 0 and i_usc  < len(row) else None
            if entr and entr > 0:
                movimenti.append({'data': data_iso, 'descrizione': descrizione, 'importo': entr, 'tipo': 'entrata'})
            elif usc and usc > 0:
                movimenti.append({'data': data_iso, 'descrizione': descrizione, 'importo': usc, 'tipo': 'uscita'})

    elif format_type == 'n26':
        i_date   = header.index('date')
        i_payee  = header.index('payee')
        i_amount = next((i for i, c in enumerate(header) if 'amount' in c and 'foreign' not in c), -1)
        i_ref    = header.index('payment reference') if 'payment reference' in header else -1
        for row in data_rows:
            if len(row) <= max(i_date, i_payee):
                continue
            data_iso = parse_date(row[i_date])
            if not data_iso:
                continue
            payee = row[i_payee].strip()
            ref   = row[i_ref].strip() if i_ref >= 0 and i_ref < len(row) else ''
            descrizione = f"{payee} - {ref}".strip(' -') if ref else payee
            if not descrizione:
                continue
            amount = parse_number(row[i_amount]) if i_amount >= 0 and i_amount < len(row) else None
            if amount is None:
                continue
            tipo = 'entrata' if amount >= 0 else 'uscita'
            movimenti.append({'data': data_iso, 'descrizione': descrizione, 'importo': abs(amount), 'tipo': tipo})

    elif format_type == 'generico':
        i_data = header.index('data')
        i_desc = header.index('descrizione')
        i_imp  = header.index('importo')
        i_tipo = header.index('tipo')      if 'tipo'      in header else -1
        i_cat  = header.index('categoria') if 'categoria' in header else -1
        for row in data_rows:
            if len(row) <= max(i_data, i_desc, i_imp):
                continue
            data_iso = parse_date(row[i_data])
            if not data_iso:
                continue
            descrizione = row[i_desc].strip()
            importo = parse_number(row[i_imp])
            if importo is None:
                continue
            if i_tipo >= 0 and i_tipo < len(row):
                tipo = row[i_tipo].strip().lower()
                if tipo not in ('entrata', 'uscita'):
                    tipo = 'entrata' if importo >= 0 else 'uscita'
                importo = abs(importo)
            else:
                tipo = 'entrata' if importo >= 0 else 'uscita'
                importo = abs(importo)
            m = {'data': data_iso, 'descrizione': descrizione, 'importo': importo, 'tipo': tipo}
            if i_cat >= 0 and i_cat < len(row) and row[i_cat].strip():
                m['categoria'] = row[i_cat].strip()
            movimenti.append(m)

    return movimenti


@app.template_filter('format_eur')
def format_eur_filter(value):
    try:
        value = float(value)
        if value == int(value):
            return f"{int(value):,}".replace(",", ".")
        else:
            formatted = f"{value:,.2f}"
            return formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(value)


# ─────────────────────────────────────────
# IMPOSTAZIONI
# ─────────────────────────────────────────

@app.route("/impostazioni/saldo", methods=["POST"])
@login_required
def imposta_saldo_iniziale():
    valore = request.form.get("saldo_iniziale", "").strip()
    # Gestione formato italiano: "40.000,50" → 40000.50 oppure "40.000" → 40000
    if "," in valore:
        # Formato europeo: punto = migliaia, virgola = decimale
        valore = valore.replace(".", "").replace(",", ".")
    elif "." in valore:
        # Solo punto: se seguito da esattamente 3 cifre finali → separatore migliaia
        import re as _re
        if _re.match(r'^\d{1,3}(\.\d{3})+$', valore):
            valore = valore.replace(".", "")
    try:
        saldo = float(valore)
    except ValueError:
        return redirect(url_for("dashboard"))
    conn = get_connection()
    conn.execute(
        "INSERT INTO impostazioni (chiave, valore) VALUES ('saldo_iniziale_conto', %s) "
        "ON CONFLICT(chiave) DO UPDATE SET valore=excluded.valore",
        (str(saldo),)
    )
    conn.commit()
    conn.close()
    anno_kpi = request.form.get("anno_kpi", "")
    return redirect(url_for("dashboard", anno_kpi=anno_kpi) if anno_kpi else url_for("dashboard"))


# ─────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    conn = get_connection()
    c = conn.cursor()

    anni_mov = [r[0] for r in c.execute(
        "SELECT DISTINCT CAST(LEFT(data, 4) AS INTEGER) FROM movimenti WHERE data IS NOT NULL ORDER BY 1 DESC"
    ).fetchall()]
    current_year = datetime.date.today().year
    if current_year not in anni_mov:
        anni_mov.insert(0, current_year)
    anni_mov = sorted(set(anni_mov), reverse=True)

    anno_kpi = int(request.args.get('anno_kpi', current_year))
    anno_kpi_str = str(anno_kpi)

    totale_entrate = c.execute(
        "SELECT COALESCE(SUM(importo), 0) FROM movimenti WHERE tipo='entrata' AND LEFT(data, 4)=%s",
        (anno_kpi_str,)
    ).fetchone()[0]

    totale_uscite = c.execute(
        "SELECT COALESCE(SUM(importo), 0) FROM movimenti WHERE tipo='uscita' AND LEFT(data, 4)=%s",
        (anno_kpi_str,)
    ).fetchone()[0]

    row = c.execute("SELECT valore FROM impostazioni WHERE chiave='saldo_iniziale_conto'").fetchone()
    saldo_iniziale = float(row[0]) if row else 0.0

    # Saldo cumulativo: parte da 5142.27 il 01/01/2026, considera solo movimenti da quella data
    SALDO_DATA_INIZIO = '2026-01-01'
    if anno_kpi < current_year:
        saldo_data_limite = f"{anno_kpi}-12-31"
    else:
        saldo_data_limite = datetime.date.today().isoformat()

    saldo = saldo_iniziale + float(c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='entrata' AND data>=%s AND data<=%s",
        (SALDO_DATA_INIZIO, saldo_data_limite)
    ).fetchone()[0]) - float(c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='uscita' AND data>=%s AND data<=%s",
        (SALDO_DATA_INIZIO, saldo_data_limite)
    ).fetchone()[0])
    num_clienti = c.execute(
        "SELECT COUNT(DISTINCT cliente_id) FROM contratti WHERE stato = 'attivo'"
    ).fetchone()[0]

    valore_pipeline = c.execute(
        "SELECT COALESCE(SUM(valore_stimato), 0) FROM opportunita WHERE stato NOT IN ('firmato', 'perso')"
    ).fetchone()[0]

    today = datetime.date.today().isoformat()
    in_30 = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()

    rate_in_scadenza = c.execute("""
        SELECT rc.*, ct.titolo, cl.nome as cliente_nome
        FROM rate_contratto rc
        JOIN contratti ct ON rc.contratto_id = ct.id
        JOIN clienti cl ON ct.cliente_id = cl.id
        WHERE rc.pagato = 0 AND rc.data_scadenza BETWEEN %s AND %s
        ORDER BY rc.data_scadenza
    """, (today, in_30)).fetchall()

    rate_scadute = c.execute("""
        SELECT rc.*, ct.titolo, cl.nome as cliente_nome, ct.id as cid
        FROM rate_contratto rc
        JOIN contratti ct ON rc.contratto_id = ct.id
        JOIN clienti cl ON ct.cliente_id = cl.id
        WHERE rc.pagato = 0 AND rc.data_scadenza < %s
        ORDER BY rc.data_scadenza
    """, (today,)).fetchall()

    ultimi_movimenti = c.execute("""
        SELECT m.*, c.nome as cliente_nome
        FROM movimenti m
        LEFT JOIN clienti c ON m.cliente_id = c.id
        ORDER BY m.data DESC, m.id DESC LIMIT 5
    """).fetchall()

    pipeline_counts = c.execute("""
        SELECT stato, COUNT(*) as cnt, COALESCE(SUM(valore_stimato), 0) as valore
        FROM opportunita
        GROUP BY stato
    """).fetchall()

    # MRR / ARR — abbonamenti attivi con almeno una rata futura non pagata
    abbonamenti_mrr = c.execute("""
        SELECT ct.id, ct.titolo, ct.percentuale_partner,
               cl.nome as cliente_nome,
               AVG(rc.importo) as rata_mensile,
               COUNT(rc.id) as rate_rimanenti,
               MAX(rc.data_scadenza) as fine_contratto
        FROM contratti ct
        JOIN clienti cl ON ct.cliente_id = cl.id
        JOIN rate_contratto rc ON rc.contratto_id = ct.id
        WHERE ct.tipo_pagamento = 'abbonamento'
          AND ct.stato = 'attivo'
          AND rc.pagato = 0
          AND rc.data_scadenza >= %s
        GROUP BY ct.id, ct.titolo, ct.percentuale_partner, cl.nome
        ORDER BY fine_contratto ASC NULLS LAST
    """, (today,)).fetchall()

    mrr = sum(a['rata_mensile'] for a in abbonamenti_mrr)
    mrr_netto = sum(
        a['rata_mensile'] * (1 - (a['percentuale_partner'] or 0) / 100)
        for a in abbonamenti_mrr
    )
    arr = mrr * 12
    in_90 = (datetime.date.today() + datetime.timedelta(days=90)).isoformat()

    anni_rows = c.execute("""
        SELECT DISTINCT CAST(LEFT(data, 4) AS INTEGER) as anno
        FROM movimenti WHERE data IS NOT NULL ORDER BY anno
    """).fetchall()
    anni_set = set(r['anno'] for r in anni_rows)
    current_year = datetime.date.today().year
    anni_set.add(current_year)
    anni_set.add(2026)
    anni_cashflow = sorted(anni_set)

    conn.close()

    return render_template("index.html",
        saldo=saldo,
        totale_entrate=totale_entrate,
        totale_uscite=totale_uscite,
        num_clienti=num_clienti,
        valore_pipeline=valore_pipeline,
        rate_in_scadenza=rate_in_scadenza,
        rate_scadute=rate_scadute,
        ultimi_movimenti=ultimi_movimenti,
        pipeline_counts=pipeline_counts,
        abbonamenti_mrr=abbonamenti_mrr,
        mrr=mrr,
        mrr_netto=mrr_netto,
        arr=arr,
        in_90=in_90,
        anni_cashflow=anni_cashflow,
        current_year=current_year,
        anni_mov=anni_mov,
        anno_kpi=anno_kpi,
        saldo_data_limite=saldo_data_limite,
        saldo_iniziale=saldo_iniziale,
    )


# ─────────────────────────────────────────
# CASHFLOW API
# ─────────────────────────────────────────

@app.route("/api/cashflow/<int:anno>")
@login_required
def api_cashflow(anno):
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            CAST(SUBSTRING(data FROM 6 FOR 2) AS INTEGER) as mese,
            tipo,
            COALESCE(categoria, 'altro') as categoria,
            SUM(importo) as totale
        FROM movimenti
        WHERE LEFT(data, 4) = %s AND data IS NOT NULL
        GROUP BY mese, tipo, categoria
        ORDER BY mese, tipo, categoria
    """, (str(anno),)).fetchall()
    conn.close()

    mesi_data = {
        i: {'entrate': 0.0, 'uscite': 0.0, 'entrate_cat': {}, 'uscite_cat': {}}
        for i in range(1, 13)
    }
    for row in rows:
        m = row['mese']
        cat = row['categoria']
        tot = row['totale']
        if row['tipo'] == 'entrata':
            mesi_data[m]['entrate'] += tot
            mesi_data[m]['entrate_cat'][cat] = mesi_data[m]['entrate_cat'].get(cat, 0.0) + tot
        else:
            mesi_data[m]['uscite'] += tot
            mesi_data[m]['uscite_cat'][cat] = mesi_data[m]['uscite_cat'].get(cat, 0.0) + tot

    result = []
    for i in range(1, 13):
        d = mesi_data[i]
        result.append({
            'mese': i,
            'entrate': round(d['entrate'], 2),
            'uscite': round(d['uscite'], 2),
            'variazione': round(d['entrate'] - d['uscite'], 2),
            'entrate_cat': {k: round(v, 2) for k, v in sorted(d['entrate_cat'].items())},
            'uscite_cat': {k: round(v, 2) for k, v in sorted(d['uscite_cat'].items())},
        })

    return jsonify({'anno': anno, 'mesi': result})


@app.route("/api/ricavi-contabili/<int:anno>")
@login_required
def api_ricavi_contabili(anno):
    conn = get_connection()
    rows = conn.execute("""
        SELECT mese, COALESCE(SUM(saldo_finale), 0) as totale
        FROM ricavi_contabili
        WHERE anno=%s AND livello=0
        GROUP BY mese
        ORDER BY mese
    """, (str(anno),)).fetchall()
    conn.close()

    mesi_data = {i: 0.0 for i in range(1, 13)}
    for row in rows:
        mesi_data[row['mese']] = round(float(row['totale']), 2)

    return jsonify({'anno': anno, 'mesi': [mesi_data[i] for i in range(1, 13)]})


# ─────────────────────────────────────────
# PIPELINE CRM
# ─────────────────────────────────────────

def parse_valore(val):
    """Converte valori in formato italiano (es. 11.000 o 11.000,50) in float."""
    s = (val or "").strip().replace(" ", "")
    if not s:
        return 0.0
    # Se c'è la virgola come decimale (es. 11.000,50 o 1.500,00)
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # Solo punti: potrebbero essere migliaia (11.000) o decimale (11.5)
        parts = s.split(".")
        if len(parts) > 1 and len(parts[-1]) == 3:
            # Ultimo gruppo ha 3 cifre → è separatore migliaia
            s = s.replace(".", "")
    return float(s)

@app.route("/pipeline")
@login_required
def pipeline():
    conn = get_connection()
    opportunita = conn.execute("""
        SELECT o.*, p.nome as partner_nome
        FROM opportunita o
        LEFT JOIN partners p ON o.partner_id = p.id
        ORDER BY o.data_creazione DESC
    """).fetchall()
    partners = conn.execute("SELECT id, nome FROM partners ORDER BY nome").fetchall()
    conn.close()

    stadi = [
        ('lead', 'Lead'),
        ('trattativa', 'Trattativa'),
        ('firmato', 'Firmato'),
        ('consegnato', 'Consegnato'),
        ('perso', 'Perso'),
    ]
    per_stadio = {s: [o for o in opportunita if o['stato'] == s] for s, _ in stadi}

    return render_template("pipeline.html",
        per_stadio=per_stadio,
        stadi=stadi,
        partners=partners,
        all_opportunita=opportunita,
    )


@app.route("/pipeline/nuovo", methods=["POST"])
@login_required
def nuova_opportunita():
    conn = get_connection()
    conn.execute("""
        INSERT INTO opportunita
            (nome_azienda, contatto, email, telefono, servizio, valore_stimato, sorgente, partner_id, stato, note)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'lead', %s)
    """, (
        request.form["nome_azienda"].strip(),
        request.form.get("contatto", "").strip(),
        request.form.get("email", "").strip(),
        request.form.get("telefono", "").strip(),
        request.form.get("servizio", "").strip(),
        parse_valore(request.form.get("valore_stimato")),
        request.form.get("sorgente", "evento"),
        request.form.get("partner_id") or None,
        request.form.get("note", "").strip(),
    ))
    conn.commit()
    conn.close()
    return redirect(url_for("pipeline"))


@app.route("/pipeline/<int:oid>/stato", methods=["POST"])
@login_required
def aggiorna_stato_opportunita(oid):
    stato = request.form["stato"]
    conn = get_connection()
    conn.execute("UPDATE opportunita SET stato=%s WHERE id=%s", (stato, oid))
    conn.commit()
    conn.close()
    return redirect(url_for("pipeline"))


@app.route("/pipeline/<int:oid>/modifica", methods=["POST"])
@login_required
def modifica_opportunita(oid):
    conn = get_connection()
    conn.execute("""
        UPDATE opportunita
        SET nome_azienda=%s, contatto=%s, email=%s, telefono=%s,
            servizio=%s, valore_stimato=%s, sorgente=%s, partner_id=%s, note=%s
        WHERE id=%s
    """, (
        request.form["nome_azienda"].strip(),
        request.form.get("contatto", "").strip(),
        request.form.get("email", "").strip(),
        request.form.get("telefono", "").strip(),
        request.form.get("servizio", "").strip(),
        parse_valore(request.form.get("valore_stimato")),
        request.form.get("sorgente", "evento"),
        request.form.get("partner_id") or None,
        request.form.get("note", "").strip(),
        oid,
    ))
    conn.commit()
    conn.close()
    return redirect(url_for("pipeline"))


@app.route("/pipeline/<int:oid>/elimina", methods=["POST"])
@login_required
def elimina_opportunita(oid):
    conn = get_connection()
    conn.execute("DELETE FROM opportunita WHERE id=%s", (oid,))
    conn.commit()
    conn.close()
    return redirect(url_for("pipeline"))


# ─────────────────────────────────────────
# PARTNER
# ─────────────────────────────────────────

@app.route("/partner")
@login_required
def partner():
    conn = get_connection()
    lista = conn.execute("""
        SELECT p.*,
               COALESCE(c_agg.num_clienti, 0)   as num_clienti,
               COALESCE(ct_agg.num_contratti, 0) as num_contratti,
               COALESCE(ct_agg.volume_totale, 0) as volume_totale,
               COALESCE(ct_agg.quota_partner, 0) as quota_partner
        FROM partners p
        LEFT JOIN (
            SELECT partner_id, COUNT(*) as num_clienti
            FROM clienti
            GROUP BY partner_id
        ) c_agg ON c_agg.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id,
                   COUNT(*) as num_contratti,
                   SUM(importo_totale) as volume_totale,
                   SUM(importo_totale * percentuale_partner / 100.0) as quota_partner
            FROM contratti
            GROUP BY partner_id
        ) ct_agg ON ct_agg.partner_id = p.id
        ORDER BY p.nome
    """).fetchall()
    conn.close()
    return render_template("partner.html", partner_list=lista)


@app.route("/partner/nuovo", methods=["POST"])
@login_required
def nuovo_partner():
    conn = get_connection()
    conn.execute("""
        INSERT INTO partners (nome, tipo, email, telefono, percentuale_default, note)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        request.form["nome"].strip(),
        request.form.get("tipo", "operativo"),
        request.form.get("email", "").strip(),
        request.form.get("telefono", "").strip(),
        float(request.form.get("percentuale_default") or 60),
        request.form.get("note", "").strip(),
    ))
    conn.commit()
    conn.close()
    return redirect(url_for("partner"))


@app.route("/partner/<int:pid>/modifica", methods=["POST"])
@login_required
def modifica_partner(pid):
    conn = get_connection()
    conn.execute("""
        UPDATE partners SET nome=%s, tipo=%s, email=%s, telefono=%s, percentuale_default=%s, note=%s
        WHERE id=%s
    """, (
        request.form["nome"].strip(),
        request.form.get("tipo", "operativo"),
        request.form.get("email", "").strip(),
        request.form.get("telefono", "").strip(),
        float(request.form.get("percentuale_default") or 60),
        request.form.get("note", "").strip(),
        pid,
    ))
    conn.commit()
    conn.close()
    return redirect(url_for("partner"))


@app.route("/partner/<int:pid>/elimina", methods=["POST"])
@login_required
def elimina_partner(pid):
    conn = get_connection()
    conn.execute("DELETE FROM partners WHERE id=%s", (pid,))
    conn.commit()
    conn.close()
    return redirect(url_for("partner"))


# ─────────────────────────────────────────
# RICAVI
# ─────────────────────────────────────────

@app.route("/ricavi")
@login_required
def ricavi():
    anno = int(request.args.get('anno', datetime.date.today().year))
    today = datetime.date.today()
    conn = get_connection()

    rows = conn.execute("""
        SELECT
            cl.id as cliente_id,
            cl.nome as cliente_nome,
            ct.id as contratto_id,
            ct.titolo,
            ct.servizio,
            ct.tipo_pagamento,
            COALESCE(ct.percentuale_partner, 0) as percentuale_partner,
            p.nome as partner_nome,
            CAST(SUBSTRING(rc.data_scadenza FROM 6 FOR 2) AS INTEGER) as mese,
            rc.id as rata_id,
            rc.importo,
            COALESCE(rc.fatturato, 0) as fatturato,
            rc.data_fatturazione,
            COALESCE(rc.pagato, 0) as pagato,
            rc.data_pagamento,
            rc.data_scadenza
        FROM rate_contratto rc
        JOIN contratti ct ON rc.contratto_id = ct.id
        JOIN clienti cl ON ct.cliente_id = cl.id
        LEFT JOIN partners p ON ct.partner_id = p.id
        WHERE LEFT(rc.data_scadenza, 4) = %s
        ORDER BY rc.data_scadenza, cl.nome
    """, (str(anno),)).fetchall()

    mesi = {}
    rate_info = {}
    totale_netto = gia_fatturato = gia_incassato = 0.0

    for r in rows:
        m = r['mese']
        if m not in mesi:
            mesi[m] = {'rate': [], 'totale': 0.0, 'fatturato': 0.0, 'incassato': 0.0, 'tot_partner': 0.0}
        importo = float(r['importo'])
        pct = float(r['percentuale_partner'])
        prodotto = r['servizio'] or r['titolo']
        quota_partner = round(importo * pct / 100, 2) if pct > 0 else 0.0
        rata = {
            'rata_id':             r['rata_id'],
            'contratto_id':        r['contratto_id'],
            'cliente_id':          r['cliente_id'],
            'cliente_nome':        r['cliente_nome'],
            'prodotto':            prodotto or '',
            'tipo_pagamento':      r['tipo_pagamento'] or 'una_tantum',
            'percentuale_partner': pct,
            'partner_nome':        r['partner_nome'] or '',
            'importo':             importo,
            'fatturato':           int(r['fatturato']),
            'data_fatturazione':   r['data_fatturazione'],
            'pagato':              int(r['pagato']),
            'data_pagamento':      r['data_pagamento'],
            'data_scadenza':       r['data_scadenza'],
        }
        mesi[m]['rate'].append(rata)
        rate_info[r['rata_id']] = rata
        mesi[m]['totale'] = round(mesi[m]['totale'] + importo, 2)
        mesi[m]['tot_partner'] = round(mesi[m]['tot_partner'] + quota_partner, 2)
        if r['fatturato']:
            mesi[m]['fatturato'] = round(mesi[m]['fatturato'] + importo, 2)
            gia_fatturato += importo
        if r['pagato']:
            mesi[m]['incassato'] = round(mesi[m]['incassato'] + importo, 2)
            gia_incassato += importo
        totale_netto += importo

    anni = [r[0] for r in conn.execute(
        "SELECT DISTINCT LEFT(data_scadenza, 4) FROM rate_contratto WHERE data_scadenza IS NOT NULL ORDER BY 1 DESC"
    ).fetchall()]
    if str(anno) not in anni:
        anni.insert(0, str(anno))

    clienti_list  = conn.execute("SELECT id, nome FROM clienti ORDER BY nome").fetchall()
    partners_list = conn.execute("SELECT id, nome, percentuale_default FROM partners ORDER BY nome").fetchall()
    conn.close()

    totale_netto   = round(totale_netto, 2)
    totale_lordo   = round(totale_netto * 1.22, 2)
    gia_fatturato  = round(gia_fatturato, 2)
    gia_incassato  = round(gia_incassato, 2)
    da_fatturare   = round(totale_netto - gia_fatturato, 2)
    da_incassare   = round(totale_netto - gia_incassato, 2)

    return render_template("ricavi.html",
        anno=anno,
        anni=anni,
        mesi=mesi,
        rate_info=rate_info,
        totale_netto=totale_netto,
        totale_lordo=totale_lordo,
        gia_fatturato=gia_fatturato,
        gia_incassato=gia_incassato,
        da_fatturare=da_fatturare,
        da_incassare=da_incassare,
        mesi_it=MESI_IT,
        today_str=today.isoformat(),
        clienti_list=clienti_list,
        partners_list=partners_list,
    )


@app.route("/ricavi/nuovo", methods=["POST"])
@login_required
def nuovo_ricavo():
    anno        = request.form.get("anno", datetime.date.today().year)
    cliente_id  = request.form.get("cliente_id", "").strip()
    nome_nuovo  = request.form.get("nome_nuovo", "").strip()
    prodotto    = request.form.get("prodotto", "").strip()
    partner_id         = request.form.get("partner_id", "").strip() or None
    partner_pct        = float(request.form.get("percentuale_partner", 0) or 0)
    nome_nuovo_partner = request.form.get("nome_nuovo_partner", "").strip()
    importo            = float(request.form["importo"])
    tipo_pagamento     = request.form.get("tipo_pagamento", "una_tantum")
    data_scad          = request.form.get("data_scadenza", "").strip()
    data_inizio        = request.form.get("data_inizio", "").strip() or data_scad
    data_firma         = request.form.get("data_firma", "").strip() or None
    durata_mesi        = max(1, int(request.form.get("durata_mesi", 12) or 12))
    num_rate           = max(2, int(request.form.get("num_rate", 4) or 4))

    conn = get_connection()
    if nome_nuovo:
        cur = conn.execute("INSERT INTO clienti (nome) VALUES (%s) RETURNING id", (nome_nuovo,))
        cliente_id = cur.fetchone()[0]

    # Crea nuovo partner al volo se richiesto
    if partner_id == "__nuovo__":
        if nome_nuovo_partner:
            cur_p = conn.execute(
                "INSERT INTO partners (nome, percentuale_default) VALUES (%s, %s) RETURNING id",
                (nome_nuovo_partner, partner_pct or 60)
            )
            partner_id = str(cur_p.fetchone()[0])
        else:
            partner_id = None
    elif partner_id and partner_pct == 0:
        p = conn.execute("SELECT percentuale_default FROM partners WHERE id=%s", (partner_id,)).fetchone()
        if p:
            partner_pct = float(p["percentuale_default"] or 0)

    # Calcola importo totale del contratto
    if tipo_pagamento == 'abbonamento':
        importo_totale = round(importo * durata_mesi, 2)
    else:
        importo_totale = importo

    cur = conn.execute("""
        INSERT INTO contratti (cliente_id, titolo, servizio, importo_totale, percentuale_partner,
                               partner_id, tipo_pagamento, data_inizio, data_firma, stato)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'attivo') RETURNING id
    """, (cliente_id, prodotto or "Ricavo", prodotto, importo_totale,
          partner_pct, partner_id, tipo_pagamento, data_inizio or data_scad, data_firma))
    contratto_id = cur.fetchone()[0]

    try:
        d = datetime.date.fromisoformat(data_inizio or data_scad)
    except Exception:
        d = datetime.date.today()

    if tipo_pagamento == 'una_tantum':
        conn.execute(
            "INSERT INTO rate_contratto (contratto_id, numero_rata, importo, data_scadenza) VALUES (%s,1,%s,%s)",
            (contratto_id, importo, data_scad or d.isoformat())
        )
    elif tipo_pagamento == 'abbonamento':
        for i in range(durata_mesi):
            mese_idx = d.month - 1 + i
            anno_r = d.year + mese_idx // 12
            mese_r = mese_idx % 12 + 1
            giorno_r = min(d.day, calendar.monthrange(anno_r, mese_r)[1])
            data_r = datetime.date(anno_r, mese_r, giorno_r).isoformat()
            conn.execute(
                "INSERT INTO rate_contratto (contratto_id, numero_rata, importo, data_scadenza, note) VALUES (%s,%s,%s,%s,%s)",
                (contratto_id, i + 1, importo, data_r, f"Canone mese {i + 1}")
            )
    elif tipo_pagamento == 'rate':
        importo_rata = round(importo / num_rate, 2)
        for i in range(num_rate):
            mese_idx = d.month - 1 + i
            anno_r = d.year + mese_idx // 12
            mese_r = mese_idx % 12 + 1
            giorno_r = min(d.day, calendar.monthrange(anno_r, mese_r)[1])
            data_r = datetime.date(anno_r, mese_r, giorno_r).isoformat()
            importo_r = importo_rata if i < num_rate - 1 else round(importo - importo_rata * (num_rate - 1), 2)
            conn.execute(
                "INSERT INTO rate_contratto (contratto_id, numero_rata, importo, data_scadenza, note) VALUES (%s,%s,%s,%s,%s)",
                (contratto_id, i + 1, importo_r, data_r, f"Rata {i + 1} di {num_rate}")
            )

    conn.commit()
    conn.close()
    return redirect(url_for("ricavi", anno=anno))


@app.route("/ricavi/rate/<int:rid>/modifica", methods=["POST"])
@login_required
def modifica_rata_ricavo(rid):
    anno          = request.form.get("anno", datetime.date.today().year)
    data_scadenza = request.form["data_scadenza"]
    importo       = float(request.form["importo"])
    cliente_nome  = request.form["cliente_nome"].strip()
    prodotto      = request.form["prodotto"].strip()
    partner_pct   = float(request.form.get("percentuale_partner", 0) or 0)
    conn = get_connection()
    rata = conn.execute(
        "SELECT contratto_id FROM rate_contratto WHERE id=%s", (rid,)
    ).fetchone()
    if rata:
        ct = conn.execute(
            "SELECT cliente_id FROM contratti WHERE id=%s", (rata["contratto_id"],)
        ).fetchone()
        conn.execute(
            "UPDATE rate_contratto SET data_scadenza=%s, importo=%s WHERE id=%s",
            (data_scadenza, importo, rid)
        )
        conn.execute(
            "UPDATE contratti SET servizio=%s, titolo=%s, percentuale_partner=%s WHERE id=%s",
            (prodotto, prodotto, partner_pct, rata["contratto_id"])
        )
        if ct:
            conn.execute("UPDATE clienti SET nome=%s WHERE id=%s", (cliente_nome, ct["cliente_id"]))
        conn.commit()
    conn.close()
    return redirect(url_for("ricavi", anno=anno))


@app.route("/ricavi/contratti/<int:cid>/elimina", methods=["POST"])
@login_required
def elimina_ricavo(cid):
    anno = request.form.get("anno", datetime.date.today().year)
    conn = get_connection()
    conn.execute("DELETE FROM rate_contratto WHERE contratto_id=%s", (cid,))
    conn.execute("DELETE FROM contratti WHERE id=%s", (cid,))
    conn.commit()
    conn.close()
    return redirect(url_for("ricavi", anno=anno))


@app.route("/rate/<int:rid>/toggle_fattura", methods=["POST"])
@login_required
def toggle_fattura(rid):
    conn = get_connection()
    rata = conn.execute("SELECT fatturato FROM rate_contratto WHERE id=%s", (rid,)).fetchone()
    if rata:
        nuovo = 0 if (rata['fatturato'] or 0) else 1
        data = datetime.date.today().isoformat() if nuovo else None
        conn.execute(
            "UPDATE rate_contratto SET fatturato=%s, data_fatturazione=%s WHERE id=%s",
            (nuovo, data, rid)
        )
        conn.commit()
        result = {'fatturato': nuovo, 'data_fatturazione': data}
    else:
        result = {'error': 'not found'}
    conn.close()
    return jsonify(result)


@app.route("/rate/<int:rid>/toggle_incasso", methods=["POST"])
@login_required
def toggle_incasso(rid):
    conn = get_connection()
    rata = conn.execute("SELECT pagato FROM rate_contratto WHERE id=%s", (rid,)).fetchone()
    if rata:
        nuovo = 0 if (rata['pagato'] or 0) else 1
        data = datetime.date.today().isoformat() if nuovo else None
        conn.execute(
            "UPDATE rate_contratto SET pagato=%s, data_pagamento=%s WHERE id=%s",
            (nuovo, data, rid)
        )
        conn.commit()
        result = {'pagato': nuovo, 'data_pagamento': data}
    else:
        result = {'error': 'not found'}
    conn.close()
    return jsonify(result)


# ─────────────────────────────────────────
# MOVIMENTI
# ─────────────────────────────────────────

@app.route("/movimenti")
@login_required
def movimenti():
    conn = get_connection()

    anni_disponibili = [r[0] for r in conn.execute(
        "SELECT DISTINCT LEFT(data, 4) as anno FROM movimenti WHERE data IS NOT NULL ORDER BY anno DESC"
    ).fetchall()]

    anno_corrente_str = str(datetime.date.today().year)
    anno_sel = request.args.get('anno', anno_corrente_str)
    mese_sel = request.args.get('mese', '')

    params = [anno_sel]
    where = "LEFT(m.data, 4) = %s"
    if mese_sel:
        where += " AND SUBSTRING(m.data FROM 6 FOR 2) = %s"
        params.append(f"{int(mese_sel):02d}")

    lista = conn.execute(f"""
        SELECT m.*, c.nome as cliente_nome
        FROM movimenti m
        LEFT JOIN clienti c ON m.cliente_id = c.id
        WHERE {where}
        ORDER BY m.data DESC, m.id DESC
    """, params).fetchall()

    clienti_list = conn.execute("SELECT id, nome FROM clienti ORDER BY nome").fetchall()
    conn.close()

    totale_entrate = sum(float(m['importo']) for m in lista if m['tipo'] == 'entrata')
    totale_uscite  = sum(float(m['importo']) for m in lista if m['tipo'] == 'uscita')

    return render_template("movimenti.html",
        movimenti=lista, clienti=clienti_list, categorie=CATEGORIE,
        anni_disponibili=anni_disponibili,
        anno_sel=anno_sel, mese_sel=mese_sel,
        totale_entrate=totale_entrate, totale_uscite=totale_uscite,
    )


@app.route("/movimenti/nuovo", methods=["POST"])
@login_required
def nuovo_movimento():
    descrizione = request.form["descrizione"].strip()
    importo = request.form["importo"]
    tipo = request.form["tipo"]
    anno_back = request.form.get("_anno", "")
    mese_back = request.form.get("_mese", "")
    if descrizione and importo:
        cliente_id = request.form.get("cliente_id") or None
        data = request.form.get("data") or None
        categoria = request.form.get("categoria") or "altro"
        conn = get_connection()
        conn.execute(
            "INSERT INTO movimenti (tipo, descrizione, importo, cliente_id, data, categoria) VALUES (%s, %s, %s, %s, %s, %s)",
            (tipo, descrizione, float(importo), cliente_id, data, categoria),
        )
        conn.commit()
        conn.close()
        # Aggiorna il filtro anno in base alla data inserita, se presente
        if data:
            anno_back = data[:4]
            mese_back = str(int(data[5:7]))
    redirect_args = {}
    if anno_back:
        redirect_args['anno'] = anno_back
    if mese_back:
        redirect_args['mese'] = mese_back
    return redirect(url_for("movimenti", **redirect_args))


@app.route("/movimenti/<int:mid>/modifica", methods=["POST"])
@login_required
def modifica_movimento(mid):
    anno_back = request.form.get("_anno", "")
    mese_back = request.form.get("_mese", "")
    data        = request.form.get("data", "").strip()
    descrizione = request.form.get("descrizione", "").strip()
    importo_raw = request.form.get("importo", "0").strip().replace(",", ".")
    tipo        = request.form.get("tipo", "uscita")
    categoria   = request.form.get("categoria", "altro")
    try:
        importo = abs(float(importo_raw))
    except ValueError:
        importo = 0.0
    conn = get_connection()
    conn.execute(
        "UPDATE movimenti SET data=%s, descrizione=%s, importo=%s, tipo=%s, categoria=%s WHERE id=%s",
        (data, descrizione, importo, tipo, categoria, mid)
    )
    conn.commit()
    conn.close()
    redirect_args = {}
    if anno_back:
        redirect_args['anno'] = anno_back
    if mese_back:
        redirect_args['mese'] = mese_back
    return redirect(url_for("movimenti", **redirect_args))


@app.route("/movimenti/<int:mid>/elimina", methods=["POST"])
@login_required
def elimina_movimento(mid):
    anno_back = request.form.get("_anno", "")
    mese_back = request.form.get("_mese", "")
    conn = get_connection()
    conn.execute("DELETE FROM movimenti WHERE id=%s", (mid,))
    conn.commit()
    conn.close()
    redirect_args = {}
    if anno_back:
        redirect_args['anno'] = anno_back
    if mese_back:
        redirect_args['mese'] = mese_back
    return redirect(url_for("movimenti", **redirect_args))


@app.route("/movimenti/elimina-tutti", methods=["POST"])
@login_required
def elimina_tutti_movimenti():
    conn = get_connection()
    conn.execute("DELETE FROM movimenti")
    conn.commit()
    conn.close()
    return redirect(url_for("movimenti"))


@app.route("/movimenti/import", methods=["GET"])
@login_required
def import_csv():
    return render_template("import_csv.html", categorie=CATEGORIE, step=1)


@app.route("/movimenti/import/upload", methods=["POST"])
@login_required
def import_csv_upload():
    f = request.files.get("csv_file")
    if not f or not f.filename:
        return render_template("import_csv.html", categorie=CATEGORIE, step=1,
                               errore="Nessun file selezionato.")
    if not f.filename.lower().endswith('.csv'):
        return render_template("import_csv.html", categorie=CATEGORIE, step=1,
                               errore="Il file deve avere estensione .csv")
    try:
        content = f.read(2 * 1024 * 1024)  # max 2MB
        righe = _parse_csv_bancario(content)
    except ValueError as e:
        return render_template("import_csv.html", categorie=CATEGORIE, step=1, errore=str(e))

    if not righe:
        return render_template("import_csv.html", categorie=CATEGORIE, step=1,
                               errore="Nessun movimento trovato nel file CSV.")

    return render_template("import_csv.html", categorie=CATEGORIE, step=2,
                           righe=righe, righe_json=json.dumps(righe))


@app.route("/movimenti/import/conferma", methods=["POST"])
@login_required
def import_csv_conferma():
    righe_json = request.form.get("righe_json", "[]")
    categoria_default = request.form.get("categoria_default", "altro")
    try:
        righe = json.loads(righe_json)
    except (json.JSONDecodeError, ValueError):
        return redirect(url_for("import_csv"))

    conn = get_connection()
    importati = 0
    duplicati = 0

    # Carico i record già presenti nel DB PRIMA del loop, così le righe
    # inserite durante questo stesso import non vengono mai considerate duplicate.
    existing_codici = set(
        r[0] for r in conn.execute(
            "SELECT codice_banca FROM movimenti WHERE codice_banca IS NOT NULL"
        ).fetchall()
    )
    existing_fallback = set(
        (r[0], r[1], round(float(r[2]), 2), r[3])
        for r in conn.execute(
            "SELECT data, tipo, importo, descrizione FROM movimenti WHERE codice_banca IS NULL"
        ).fetchall()
    )

    for r in righe:
        codice_banca = r.get('codice_banca') or None
        if codice_banca:
            if codice_banca in existing_codici:
                duplicati += 1
                continue
        else:
            key = (r['data'], r['tipo'], round(float(r['importo']), 2), r['descrizione'])
            if key in existing_fallback:
                duplicati += 1
                continue
        categoria = r.get('categoria') or categoria_default
        conn.execute(
            "INSERT INTO movimenti (tipo, descrizione, importo, data, categoria, codice_banca) VALUES (%s, %s, %s, %s, %s, %s)",
            (r['tipo'], r['descrizione'], float(r['importo']), r['data'], categoria, codice_banca)
        )
        importati += 1
    conn.commit()
    conn.close()

    anni_importati = sorted({r['data'][:4] for r in righe if r.get('data') and len(r['data']) >= 4}, reverse=True)
    anno_importato = anni_importati[0] if anni_importati else str(datetime.date.today().year)

    return render_template("import_csv.html", categorie=CATEGORIE, step=3,
                           importati=importati, duplicati=duplicati,
                           anno_importato=anno_importato)


@app.route("/movimenti/import/template")
@login_required
def import_csv_template():
    contenuto = "data,descrizione,importo,tipo,categoria\n"
    contenuto += "2024-01-15,Pagamento cliente Rossi,1200.00,entrata,contratti\n"
    contenuto += "2024-01-20,Abbonamento Google Workspace,14.40,uscita,software\n"
    contenuto += "2024-01-25,Affitto ufficio gennaio,800.00,uscita,affitto\n"
    return Response(
        contenuto,
        mimetype='text/csv',
        headers={"Content-Disposition": "attachment; filename=template_movimenti.csv"}
    )


# ─────────────────────────────────────────
# PREVENTIVO (stampa / PDF)
# ─────────────────────────────────────────

@app.route("/preventivo/<int:cid>")
@login_required
def preventivo(cid):
    conn = get_connection()
    contratto = conn.execute("""
        SELECT ct.*, cl.nome as cliente_nome, cl.email as cliente_email,
               cl.telefono as cliente_tel, p.nome as partner_nome
        FROM contratti ct
        JOIN clienti cl ON ct.cliente_id = cl.id
        LEFT JOIN partners p ON ct.partner_id = p.id
        WHERE ct.id = %s
    """, (cid,)).fetchone()

    if not contratto:
        conn.close()
        return redirect(url_for("contratti"))

    rate = conn.execute(
        "SELECT * FROM rate_contratto WHERE contratto_id=%s ORDER BY numero_rata",
        (cid,)
    ).fetchall()
    conn.close()

    nostro_quota = contratto['importo_totale'] * (1 - contratto['percentuale_partner'] / 100)
    oggi = datetime.date.today().strftime("%d/%m/%Y")

    return render_template("preventivo_print.html",
        contratto=contratto,
        rate=rate,
        nostro_quota=nostro_quota,
        oggi=oggi,
    )




# ─────────────────────────────────────────────
#  COSTI CONTABILI (import da Profis)
# ─────────────────────────────────────────────

def _parse_csv_profis(content_bytes, anno, mese):
    """
    Analizza un CSV esportato da Profis e restituisce una lista di dict:
    {conto, livello, flag, descrizione, note, saldo_non_rettificato, rettifiche, saldo_finale}
    Formato: Conto;COLONNA2;Descrizione;COLONNA4;Saldo non rettificato;Rettifiche;Saldo finale
    """
    text = None
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1'):
        try:
            text = content_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("Impossibile decodificare il file.")

    def parse_num(s):
        if not s or not s.strip():
            return None
        s = s.strip().replace('.', '').replace(',', '.')
        try:
            return float(s)
        except ValueError:
            return None

    reader = csv.reader(io.StringIO(text), delimiter=';')
    rows = list(reader)

    # Trova la riga header
    header_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip().lower() == 'conto':
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Intestazione 'Conto' non trovata nel file.")

    voci = []
    for row in rows[header_idx + 1:]:
        if not row or not any(c.strip() for c in row):
            continue
        conto_raw = row[0] if len(row) > 0 else ''
        # Calcola livello in base agli spazi iniziali (ogni 4 spazi = 1 livello)
        leading = len(conto_raw) - len(conto_raw.lstrip(' '))
        livello = leading // 4
        conto = conto_raw.strip()
        if not conto:
            continue
        flag        = row[1].strip() if len(row) > 1 else ''
        descrizione = row[2].strip() if len(row) > 2 else ''
        note        = row[3].strip() if len(row) > 3 else ''
        snr         = parse_num(row[4]) if len(row) > 4 else None
        rett        = parse_num(row[5]) if len(row) > 5 else None
        saldo       = parse_num(row[6]) if len(row) > 6 else None

        if saldo is None:
            continue

        voci.append({
            'anno': anno,
            'mese': mese,
            'conto': conto,
            'livello': livello,
            'flag': flag or None,
            'descrizione': descrizione,
            'note': note or None,
            'saldo_non_rettificato': snr,
            'rettifiche': rett,
            'saldo_finale': saldo,
        })
    return voci


@app.route('/costi-contabili')
@login_required
def costi_contabili():
    conn = get_connection()
    # Mesi disponibili
    mesi_disponibili = conn.execute("""
        SELECT DISTINCT anno, mese FROM costi_contabili ORDER BY anno DESC, mese DESC
    """).fetchall()

    anno = request.args.get('anno', type=int)
    mese = request.args.get('mese', type=int)

    voci = []
    totale = 0.0
    if anno and mese:
        voci = conn.execute("""
            SELECT * FROM costi_contabili
            WHERE anno=%s AND mese=%s
            ORDER BY conto
        """, (anno, mese)).fetchall()
        totale = sum(v['saldo_finale'] for v in voci if v['livello'] == 0)
    conn.close()

    return render_template('costi_contabili.html',
        mesi_disponibili=mesi_disponibili,
        anno=anno,
        mese=mese,
        voci=voci,
        totale=totale,
        MESI_IT=MESI_IT,
    )


@app.route('/ricavi-contabili/import', methods=['GET', 'POST'])
@login_required
def ricavi_contabili_import():
    errore = None
    if request.method == 'POST':
        f = request.files.get('csv_file')
        anno = request.form.get('anno', type=int)
        mese = request.form.get('mese', type=int)
        if not f or not anno or not mese:
            errore = "Seleziona il file, l'anno e il mese."
        else:
            try:
                voci = _parse_csv_profis(f.read(), anno, mese)
                conn = get_connection()
                inseriti = 0
                sostituiti = 0
                for v in voci:
                    existing = conn.execute(
                        "SELECT id FROM ricavi_contabili WHERE anno=%s AND mese=%s AND conto=%s",
                        (v['anno'], v['mese'], v['conto'])
                    ).fetchone()
                    if existing:
                        conn.execute("""
                            UPDATE ricavi_contabili SET livello=%s, flag=%s, descrizione=%s, note=%s,
                            saldo_non_rettificato=%s, rettifiche=%s, saldo_finale=%s,
                            importato_il=to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
                            WHERE anno=%s AND mese=%s AND conto=%s
                        """, (v['livello'], v['flag'], v['descrizione'], v['note'],
                              v['saldo_non_rettificato'], v['rettifiche'], v['saldo_finale'],
                              v['anno'], v['mese'], v['conto']))
                        sostituiti += 1
                    else:
                        conn.execute("""
                            INSERT INTO ricavi_contabili
                            (anno, mese, conto, livello, flag, descrizione, note,
                             saldo_non_rettificato, rettifiche, saldo_finale)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (v['anno'], v['mese'], v['conto'], v['livello'], v['flag'],
                              v['descrizione'], v['note'],
                              v['saldo_non_rettificato'], v['rettifiche'], v['saldo_finale']))
                        inseriti += 1
                conn.commit()
                conn.close()
                return redirect(url_for('pl', anno=anno, mese=mese))
            except Exception as e:
                errore = str(e)

    anno_corrente = datetime.date.today().year
    return render_template('ricavi_contabili_import.html',
        errore=errore,
        anno_corrente=anno_corrente,
        MESI_IT=MESI_IT,
    )


@app.route('/costi-contabili/import', methods=['GET', 'POST'])
@login_required
def costi_contabili_import():
    errore = None
    if request.method == 'POST':
        f = request.files.get('csv_file')
        anno = request.form.get('anno', type=int)
        mese = request.form.get('mese', type=int)
        if not f or not anno or not mese:
            errore = "Seleziona il file, l'anno e il mese."
        else:
            try:
                voci = _parse_csv_profis(f.read(), anno, mese)
                conn = get_connection()
                inseriti = 0
                sostituiti = 0
                for v in voci:
                    existing = conn.execute(
                        "SELECT id FROM costi_contabili WHERE anno=%s AND mese=%s AND conto=%s",
                        (v['anno'], v['mese'], v['conto'])
                    ).fetchone()
                    if existing:
                        conn.execute("""
                            UPDATE costi_contabili SET livello=%s, flag=%s, descrizione=%s, note=%s,
                            saldo_non_rettificato=%s, rettifiche=%s, saldo_finale=%s,
                            importato_il=to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
                            WHERE anno=%s AND mese=%s AND conto=%s
                        """, (v['livello'], v['flag'], v['descrizione'], v['note'],
                              v['saldo_non_rettificato'], v['rettifiche'], v['saldo_finale'],
                              v['anno'], v['mese'], v['conto']))
                        sostituiti += 1
                    else:
                        conn.execute("""
                            INSERT INTO costi_contabili
                            (anno, mese, conto, livello, flag, descrizione, note,
                             saldo_non_rettificato, rettifiche, saldo_finale)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """, (v['anno'], v['mese'], v['conto'], v['livello'], v['flag'],
                              v['descrizione'], v['note'],
                              v['saldo_non_rettificato'], v['rettifiche'], v['saldo_finale']))
                        inseriti += 1
                conn.commit()
                conn.close()
                return redirect(url_for('pl', anno=anno, mese=mese))
            except Exception as e:
                errore = str(e)

    anno_corrente = datetime.date.today().year
    return render_template('costi_contabili_import.html',
        errore=errore,
        anno_corrente=anno_corrente,
        MESI_IT=MESI_IT,
    )


@app.route('/costi-contabili/elimina/<int:anno>/<int:mese>', methods=['POST'])
@login_required
def costi_contabili_elimina_mese(anno, mese):
    conn = get_connection()
    conn.execute("DELETE FROM costi_contabili WHERE anno=%s AND mese=%s", (anno, mese))
    conn.commit()
    conn.close()
    return redirect(url_for('pl'))


@app.route('/ricavi-contabili/elimina/<int:anno>/<int:mese>', methods=['POST'])
@login_required
def ricavi_contabili_elimina_mese(anno, mese):
    conn = get_connection()
    conn.execute("DELETE FROM ricavi_contabili WHERE anno=%s AND mese=%s", (anno, mese))
    conn.commit()
    conn.close()
    return redirect(url_for('pl'))


# ─────────────────────────────────────────
# COSTI ANNO
# ─────────────────────────────────────────

@app.route("/scadenze-costi/nuovo", methods=["POST"])
@login_required
def nuovo_scadenza_costo():
    nome = request.form["nome"]
    categoria = request.form["categoria"]
    importo_rata = float(request.form["importo_rata"])
    uscita_cassa_rata = float(request.form.get("uscita_cassa_rata") or 0)
    ricorrenza = request.form["ricorrenza"]
    data_prima = request.form["data_prima_scadenza"]
    note = request.form.get("note", "")

    num_rate_raw = request.form.get("num_rate", "").strip()
    data_fine_raw = request.form.get("data_fine", "").strip()
    if num_rate_raw:
        num_rate = int(num_rate_raw)
    elif data_fine_raw:
        data_fine = datetime.date.fromisoformat(data_fine_raw)
        data_inizio = datetime.date.fromisoformat(data_prima)
        mesi_interval = RICORRENZE.get(ricorrenza, 1)
        num_rate = 0
        d = data_inizio
        while d <= data_fine:
            num_rate += 1
            d = _add_months(d, mesi_interval)
        if num_rate == 0:
            num_rate = 1
    else:
        num_rate = 1

    conn = get_connection()
    cur = conn.execute("""
        INSERT INTO scadenze_costi (nome, categoria, importo_rata, uscita_cassa_rata, ricorrenza, data_prima_scadenza, num_rate, note)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """, (nome, categoria, importo_rata, uscita_cassa_rata, ricorrenza, data_prima, num_rate, note))
    scadenza_id = cur.fetchone()[0]
    _genera_rate_costo(conn, scadenza_id, data_prima, ricorrenza, num_rate, importo_rata, uscita_cassa_rata)
    conn.commit()
    conn.close()
    return redirect(url_for("costi_anno", anno=datetime.date.fromisoformat(data_prima).year))


@app.route("/scadenze-costi/rate/<int:rid>/toggle_pagamento", methods=["POST"])
@login_required
def toggle_pagamento_costo(rid):
    conn = get_connection()
    rata = conn.execute("SELECT pagato FROM rate_scadenza_costo WHERE id=%s", (rid,)).fetchone()
    if rata:
        nuovo = 0 if (rata["pagato"] or 0) else 1
        data = datetime.date.today().isoformat() if nuovo else None
        conn.execute(
            "UPDATE rate_scadenza_costo SET pagato=%s, data_pagamento=%s WHERE id=%s",
            (nuovo, data, rid)
        )
        conn.commit()
        result = {"pagato": nuovo, "data_pagamento": data}
    else:
        result = {"error": "not found"}
    conn.close()
    return jsonify(result)


@app.route("/costi-anno")
@login_required
def costi_anno():
    anno = int(request.args.get("anno", datetime.date.today().year))
    today = datetime.date.today()
    conn = get_connection()

    rows = conn.execute("""
        SELECT
            sc.id as costo_id,
            sc.nome,
            sc.categoria,
            sc.note,
            CAST(SUBSTRING(rsc.data_scadenza FROM 6 FOR 2) AS INTEGER) as mese,
            rsc.id as rata_id,
            rsc.importo,
            COALESCE(rsc.uscita_cassa, 0) as uscita_cassa,
            COALESCE(rsc.pagato, 0) as pagato,
            rsc.data_pagamento,
            rsc.data_scadenza
        FROM rate_scadenza_costo rsc
        JOIN scadenze_costi sc ON rsc.scadenza_costo_id = sc.id
        WHERE LEFT(rsc.data_scadenza, 4) = %s
        ORDER BY rsc.data_scadenza, sc.nome
    """, (str(anno),)).fetchall()

    # Raggruppa per mese + dizionario costi unici per il modal di modifica
    mesi = {}
    costi_unici = {}
    totale_anno = 0.0
    pagato_anno = 0.0
    uscita_cassa_anno = 0.0
    uscita_cassa_pagata_anno = 0.0
    for r in rows:
        m = r["mese"]
        if m not in mesi:
            mesi[m] = {"rate": [], "totale": 0.0, "pagato": 0.0, "uscita_cassa": 0.0, "uscita_cassa_pagata": 0.0}
        importo = float(r["importo"])
        uscita_cassa = float(r["uscita_cassa"])
        mesi[m]["rate"].append({
            "rata_id": r["rata_id"],
            "costo_id": r["costo_id"],
            "nome": r["nome"],
            "categoria": r["categoria"],
            "importo": importo,
            "uscita_cassa": uscita_cassa,
            "pagato": int(r["pagato"]),
            "data_pagamento": r["data_pagamento"],
            "data_scadenza": r["data_scadenza"],
        })
        if r["costo_id"] not in costi_unici:
            costi_unici[r["costo_id"]] = {
                "id": r["costo_id"],
                "nome": r["nome"],
                "categoria": r["categoria"],
                "note": r["note"] or "",
            }
        mesi[m]["totale"] = round(mesi[m]["totale"] + importo, 2)
        mesi[m]["uscita_cassa"] = round(mesi[m]["uscita_cassa"] + uscita_cassa, 2)
        if r["pagato"]:
            mesi[m]["pagato"] = round(mesi[m]["pagato"] + importo, 2)
            mesi[m]["uscita_cassa_pagata"] = round(mesi[m]["uscita_cassa_pagata"] + uscita_cassa, 2)
            pagato_anno += importo
            uscita_cassa_pagata_anno += uscita_cassa
        totale_anno += importo
        uscita_cassa_anno += uscita_cassa

    anni = [r[0] for r in conn.execute(
        "SELECT DISTINCT LEFT(data_scadenza, 4) FROM rate_scadenza_costo WHERE data_scadenza IS NOT NULL ORDER BY 1 DESC"
    ).fetchall()]
    if str(anno) not in anni:
        anni.insert(0, str(anno))

    conn.close()
    return render_template("costi_anno.html",
        anno=anno,
        anni=anni,
        mesi=mesi,
        costi_unici=costi_unici,
        totale_anno=round(totale_anno, 2),
        pagato_anno=round(pagato_anno, 2),
        uscita_cassa_anno=round(uscita_cassa_anno, 2),
        uscita_cassa_pagata_anno=round(uscita_cassa_pagata_anno, 2),
        mesi_it=MESI_IT,
        today_str=today.isoformat(),
        categorie=CATEGORIE_COSTI,
        ricorrenze=list(RICORRENZE.keys()),
    )


@app.route("/costi/rate/<int:rid>/modifica", methods=["POST"])
@login_required
def modifica_rata_costo(rid):
    nome = request.form["nome"]
    categoria = request.form["categoria"]
    note = request.form.get("note", "")
    data_scadenza = request.form["data_scadenza"]
    importo = float(request.form["importo"])
    uscita_cassa = float(request.form.get("uscita_cassa") or 0)
    anno = request.form.get("anno", datetime.date.today().year)
    conn = get_connection()
    rata = conn.execute("SELECT scadenza_costo_id FROM rate_scadenza_costo WHERE id=%s", (rid,)).fetchone()
    if rata:
        conn.execute(
            "UPDATE rate_scadenza_costo SET data_scadenza=%s, importo=%s, uscita_cassa=%s WHERE id=%s",
            (data_scadenza, importo, uscita_cassa, rid)
        )
        conn.execute(
            "UPDATE scadenze_costi SET nome=%s, categoria=%s, note=%s WHERE id=%s",
            (nome, categoria, note, rata["scadenza_costo_id"])
        )
        conn.commit()
    conn.close()
    return redirect(url_for("costi_anno", anno=anno))


@app.route("/costi/<int:cid>/elimina", methods=["POST"])
@login_required
def elimina_costo(cid):
    conn = get_connection()
    conn.execute("DELETE FROM rate_scadenza_costo WHERE scadenza_costo_id=%s", (cid,))
    conn.execute("DELETE FROM scadenze_costi WHERE id=%s", (cid,))
    conn.commit()
    conn.close()
    anno = request.form.get("anno", datetime.date.today().year)
    return redirect(url_for("costi_anno", anno=anno))


@app.route("/costi-anno/azzera", methods=["POST"])
@login_required
def azzera_costi_anno():
    anno = int(request.form.get("anno", datetime.date.today().year))
    conn = get_connection()

    # 1. Raccoglie gli hash CSV delle rate dell'anno prima di eliminarle
    hashes = [r[0] for r in conn.execute("""
        SELECT csv_hash FROM rate_scadenza_costo
        WHERE csv_hash IS NOT NULL
          AND LEFT(data_scadenza, 4) = %s
    """, (str(anno),)).fetchall()]

    # 2. Elimina le rate dell'anno
    conn.execute("""
        DELETE FROM rate_scadenza_costo
        WHERE LEFT(data_scadenza, 4) = %s
    """, (str(anno),))

    # 3. Elimina le scadenze rimaste senza rate (orfane)
    conn.execute("""
        DELETE FROM scadenze_costi
        WHERE id NOT IN (SELECT DISTINCT scadenza_costo_id FROM rate_scadenza_costo)
    """)

    # 4. Elimina i movimenti creati dalla riconciliazione CSV per quell'anno
    #    (riconoscibili dal codice_banca = 'csv_<hash>')
    if hashes:
        placeholders = ",".join(["%s"] * len(hashes))
        conn.execute(
            f"DELETE FROM movimenti WHERE codice_banca IN ({placeholders})",
            hashes
        )
    # Fallback: rimuove anche uscite con categoria='costi' senza codice_banca dell'anno
    conn.execute("""
        DELETE FROM movimenti
        WHERE tipo='uscita' AND categoria='costi'
          AND LEFT(data, 4) = %s
          AND (codice_banca IS NULL OR codice_banca NOT LIKE 'csv_%%')
    """, (str(anno),))

    conn.commit()
    conn.close()
    return redirect(url_for("costi_anno", anno=anno))


# ─────────────────────────────────────────
# RICONCILIAZIONE CSV
# ─────────────────────────────────────────

def _auto_salva_regola(conn, descrizione, nome_costo, categoria, scadenza_id=None, importo_esatto=None):
    """Salva automaticamente una regola di riconciliazione se non esiste già."""
    desc = descrizione.strip()
    pattern = None

    # Pattern Banca Sella: "... A NOME FORNITORE" → usa "NOME FORNITORE" lowercase
    if ' A ' in desc.upper():
        after_a = desc.upper().rsplit(' A ', 1)[-1].strip()
        parole = [w for w in after_a.split() if len(w) > 3]
        if parole:
            pattern = ' '.join(parole[:2]).lower()

    # Fallback: prime 2 parole significative della descrizione
    if not pattern:
        parole = [w for w in desc.split() if len(w) > 3]
        if parole:
            pattern = ' '.join(parole[:2]).lower()

    if not pattern or len(pattern) < 4:
        return

    if scadenza_id:
        # Pattern specifico al fornitore/costo%s (le parole del nome appaiono nel pattern)
        # → non serve importo_esatto, il pattern è già discriminante
        nome_words = [w.lower() for w in nome_costo.split() if len(w) > 3]
        pattern_specifico = any(w in pattern for w in nome_words)
        imp = None if pattern_specifico else importo_esatto

        esistente = conn.execute(
            "SELECT id FROM riconciliazione_regole WHERE pattern=%s AND scadenza_id=%s AND (importo_esatto=%s OR importo_esatto IS NULL)",
            (pattern, scadenza_id, imp)
        ).fetchone()
        if not esistente:
            conn.execute("""
                INSERT INTO riconciliazione_regole
                    (pattern, importo_esatto, nome_costo, categoria, raggruppa_per_mese, scadenza_id)
                VALUES (%s, %s, %s, %s, 0, %s)
            """, (pattern, imp, nome_costo, categoria, scadenza_id))
    else:
        esistente = conn.execute(
            "SELECT id FROM riconciliazione_regole WHERE pattern=%s AND scadenza_id IS NULL",
            (pattern,)
        ).fetchone()
        if not esistente:
            conn.execute("""
                INSERT INTO riconciliazione_regole (pattern, nome_costo, categoria, raggruppa_per_mese)
                VALUES (%s, %s, %s, 0)
            """, (pattern, nome_costo, categoria))


def _csv_hash(data, importo, descrizione):
    s = f"{data}|{importo:.2f}|{descrizione}"
    return hashlib.md5(s.encode()).hexdigest()


def _mese_precedente(data_iso):
    """Ritorna la stringa 'YYYY-MM' del mese precedente a data_iso."""
    d = datetime.date.fromisoformat(data_iso)
    if d.month == 1:
        return f"{d.year - 1}-12"
    return f"{d.year}-{d.month - 1:02d}"


_PREFISSI_COMMISSIONE = (
    'comm.bon', 'commissioni', 'spese bonifico', 'spese bon',
    'commissione bonifico', 'oneri bancari',
)


def _is_commissione(descrizione):
    """True se la descrizione è una commissione/spesa bancaria (es. Comm.Bon.Altra Banca A ...)."""
    d = descrizione.strip().lower()
    return any(d.startswith(p) or d[:30].startswith(p) for p in _PREFISSI_COMMISSIONE)


def _estrai_nome_desc(descrizione):
    """Estrae le parole-chiave identificative (nome fornitore) da una descrizione bancaria.
    Ritorna lista di token uppercase con len > 2 dalla parte finale dopo ' A '.
    """
    import re
    desc = descrizione.strip().upper()
    m = re.search(r'\bA\s+([A-Z][A-Z\s\.]{3,60})$', desc)
    if m:
        return [w for w in m.group(1).split() if len(w) > 2]
    return []


def _trova_match_storico(conn, descrizione, importo, data_csv):
    """Cerca nel mese precedente una corrispondenza per:
    1. Nome fornitore — solo tra righe dello STESSO TIPO (commissione vs fattura/bonifico).
       Due righe con lo stesso nome ma tipo diverso (es. commissione €0,75 e fattura €600)
       non si mescolano mai.
    2. Descrizione esatta + importo esatto — per descrizioni generiche senza nome.

    Ritorna (nome, categoria) oppure (None, None) se non trovato o ambiguo.
    """
    mese_prec = _mese_precedente(data_csv)

    storico = conn.execute("""
        SELECT sc.nome, sc.categoria, rsc.csv_descrizione,
               COALESCE(rsc.uscita_cassa, 0) as uscita_cassa
        FROM rate_scadenza_costo rsc
        JOIN scadenze_costi sc ON rsc.scadenza_costo_id = sc.id
        WHERE LEFT(rsc.data_pagamento, 7) = %s
          AND COALESCE(rsc.pagato, 0) = 1
          AND rsc.csv_descrizione IS NOT NULL
    """, (mese_prec,)).fetchall()

    if not storico:
        # Fallback: cerca nei nomi delle scadenze del mese precedente
        # (funziona anche per record senza csv_descrizione)
        return _trova_match_per_nome(conn, descrizione, importo, data_csv)

    curr_is_comm = _is_commissione(descrizione)

    # Separa record con e senza csv_descrizione
    storico_con_desc = [r for r in storico if r['csv_descrizione']]
    storico_senza_desc = [r for r in storico if not r['csv_descrizione']]

    # 1. Match per nome estratto dalla descrizione CSV storica — stesso tipo
    parole = _estrai_nome_desc(descrizione)
    if parole and storico_con_desc:
        matches = []
        for row in storico_con_desc:
            if _is_commissione(row['csv_descrizione']) != curr_is_comm:
                continue
            prev = row['csv_descrizione'].upper()
            if all(w in prev for w in parole):
                matches.append(row)
        if len(matches) == 1:
            return matches[0]['nome'], matches[0]['categoria']
        if len(matches) > 1:
            return None, None  # ambiguo

    # 2. Match per descrizione esatta + importo esatto (es. PagoPA, canoni fissi)
    desc_norm = descrizione.strip().lower()
    matches = [
        row for row in storico_con_desc
        if row['csv_descrizione'].strip().lower() == desc_norm
        and abs(float(row['uscita_cassa']) - importo) < 0.01
    ]
    if len(matches) == 1:
        return matches[0]['nome'], matches[0]['categoria']

    # 3. Fallback per record senza csv_descrizione:
    #    cerca token del nome assegnato (scadenza.nome) nella descrizione corrente
    if storico_senza_desc and not curr_is_comm:
        return _trova_match_per_nome(conn, descrizione, importo, data_csv)

    return None, None


def _trova_match_per_nome(conn, descrizione, importo, data_csv):
    """Fallback: cerca nei nomi delle scadenze del mese precedente i cui token
    appaiono nella descrizione CSV corrente. Non usato per commissioni.
    """
    if _is_commissione(descrizione):
        return None, None

    mese_prec = _mese_precedente(data_csv)
    desc_upper = descrizione.upper()

    candidati = conn.execute("""
        SELECT DISTINCT sc.nome, sc.categoria
        FROM rate_scadenza_costo rsc
        JOIN scadenze_costi sc ON rsc.scadenza_costo_id = sc.id
        WHERE LEFT(rsc.data_pagamento, 7) = %s
          AND COALESCE(rsc.pagato, 0) = 1
    """, (mese_prec,)).fetchall()

    matches = []
    for row in candidati:
        # Salta categorie chiaramente bancarie
        if row['categoria'] in ('banca',):
            continue
        # I token del nome assegnato devono apparire tutti nella descrizione CSV
        tokens = [w.upper() for w in row['nome'].split() if len(w) > 2]
        if tokens and all(t in desc_upper for t in tokens):
            matches.append(row)

    if len(matches) == 1:
        return matches[0]['nome'], matches[0]['categoria']
    return None, None


def _nome_da_descrizione(descrizione):
    """Estrae un nome leggibile dalla descrizione bancaria grezza."""
    desc = descrizione.strip()
    # Pattern "... A NOME COGNOME" alla fine
    import re
    m = re.search(r'\bA\s+([A-Z][A-Z\s]{3,40})$', desc.upper())
    if m:
        return m.group(1).strip().title()
    # Rimuovi prefissi comuni
    for prefix in ('PAGAMENTO FATTURA', 'PAGAMENTO ', 'ADDEBITO ', 'BONIFICO ',
                   'C/C ', 'RID ', 'SDD '):
        if desc.upper().startswith(prefix):
            rest = desc[len(prefix):].strip()
            if rest:
                return rest[:60]
    return desc[:60]


def _riconcilia_uscite(conn, uscite):
    """Divide le uscite CSV in auto_match (rata esistente) e da_assegnare (tutto il resto).
    Il nome/categoria vengono pre-suggeriti dalla regola se presente."""
    regole = conn.execute("SELECT * FROM riconciliazione_regole ORDER BY id").fetchall()
    rate_pending = conn.execute("""
        SELECT rsc.id as rata_id, rsc.importo, rsc.data_scadenza,
               sc.nome, sc.categoria, COALESCE(rsc.pagato,0) as pagato
        FROM rate_scadenza_costo rsc
        JOIN scadenze_costi sc ON rsc.scadenza_costo_id = sc.id
        WHERE COALESCE(rsc.uscita_cassa, 0) = 0
        ORDER BY rsc.data_scadenza
    """).fetchall()

    auto_match = []
    da_assegnare = []
    rate_usate = set()

    for u in uscite:
        h = _csv_hash(u['data'], u['importo'], u['descrizione'])

        # Già riconciliato: salta
        if conn.execute("SELECT id FROM rate_scadenza_costo WHERE csv_hash=%s", (h,)).fetchone():
            continue
        if conn.execute("SELECT id FROM movimenti WHERE codice_banca=%s", (f"csv_{h}",)).fetchone():
            continue
        already_paid = conn.execute("""
            SELECT rsc.id FROM rate_scadenza_costo rsc
            WHERE COALESCE(rsc.pagato, 0) = 1
              AND ROUND(COALESCE(rsc.uscita_cassa, 0)::numeric, 2) = ROUND(%s::numeric, 2)
              AND rsc.data_pagamento = %s
        """, (u['importo'], u['data'])).fetchall()
        if already_paid:
            desc_upper = u['descrizione'].upper()
            skip = False
            for row in already_paid:
                sc_row = conn.execute(
                    "SELECT nome FROM scadenze_costi WHERE id = "
                    "(SELECT scadenza_costo_id FROM rate_scadenza_costo WHERE id=%s)",
                    (row['id'],)
                ).fetchone()
                if sc_row:
                    nome_upper = sc_row['nome'].upper()
                    parole = [w for w in nome_upper.split() if len(w) > 3]
                    if parole and all(w in desc_upper for w in parole):
                        skip = True; break
                    if any(w in nome_upper for w in desc_upper.split() if len(w) > 3):
                        skip = True; break
            if skip:
                continue

        # Auto-match a rata esistente
        data_usc = datetime.date.fromisoformat(u['data'])
        matched_rata = None
        for r in rate_pending:
            if r['rata_id'] in rate_usate:
                continue
            if abs(float(r['importo']) - u['importo']) > 0.01:
                continue
            if not r['data_scadenza']:
                continue
            if abs((data_usc - datetime.date.fromisoformat(r['data_scadenza'])).days) <= 30:
                matched_rata = r
                break

        if matched_rata:
            rate_usate.add(matched_rata['rata_id'])
            auto_match.append({
                'hash': h, 'data': u['data'], 'descrizione': u['descrizione'],
                'importo': u['importo'], 'rata_id': matched_rata['rata_id'],
                'costo_nome': matched_rata['nome'], 'categoria': matched_rata['categoria'],
            })
            continue

        # 1. Prova match storico sul mese precedente (priorità massima)
        nome_sug, cat_sug = _trova_match_storico(conn, u['descrizione'], u['importo'], u['data'])
        storico_trovato = nome_sug is not None

        # 2. Fallback: regola di riconciliazione salvata
        # Le regole di categoria 'banca' si applicano SOLO a descrizioni commissione/bancarie,
        # non a fatture o bonifici verso fornitori che contengono lo stesso nome.
        if not nome_sug:
            curr_is_comm = _is_commissione(u['descrizione'])
            for reg in regole:
                if reg['pattern'].lower() in u['descrizione'].lower():
                    if reg['importo_esatto'] is None or abs(float(reg['importo_esatto']) - u['importo']) < 0.01:
                        if reg['categoria'] == 'banca' and not curr_is_comm:
                            continue  # regola bancaria non si applica a fatture
                        nome_sug = reg['nome_costo']
                        cat_sug = reg['categoria']
                        break

        # 3. Fallback finale: estrai il nome dalla descrizione bancaria
        if not nome_sug:
            nome_sug = _nome_da_descrizione(u['descrizione'])
            cat_sug = 'altro'

        da_assegnare.append({
            'hash': h, 'data': u['data'], 'descrizione': u['descrizione'],
            'importo': u['importo'],
            'nome': nome_sug,
            'categoria': cat_sug or 'altro',
            'storico': storico_trovato,  # True = match da mese precedente
        })

    return auto_match, da_assegnare


def _upsert_movimento(conn, data, descrizione, importo, csv_hash):
    """Aggiunge il movimento alla tabella movimenti se non già presente."""
    # Controlla per hash riconciliazione
    if csv_hash:
        if conn.execute("SELECT id FROM movimenti WHERE codice_banca=%s", (f"csv_{csv_hash}",)).fetchone():
            return
    # Controlla per data + importo + tipo (movimenti già importati dal CSV normale)
    if conn.execute(
        "SELECT id FROM movimenti WHERE data=%s AND importo=%s AND tipo='uscita'",
        (data, importo)
    ).fetchone():
        return
    conn.execute(
        "INSERT INTO movimenti (tipo, descrizione, importo, data, categoria, codice_banca) VALUES (%s, %s, %s, %s, %s, %s)",
        ('uscita', descrizione, importo, data, 'costi', f"csv_{csv_hash}" if csv_hash else None)
    )


@app.route("/costi-anno/riconcilia", methods=["GET", "POST"])
@login_required
def riconcilia_costi():
    anno = int(request.args.get("anno", datetime.date.today().year))
    if request.method == "GET":
        return render_template("costi_riconcilia.html", step=1, anno=anno)

    f = request.files.get("csv_file")
    if not f or not f.filename:
        return render_template("costi_riconcilia.html", step=1, anno=anno,
                               errore="Nessun file selezionato.")
    if not f.filename.lower().endswith('.csv'):
        return render_template("costi_riconcilia.html", step=1, anno=anno,
                               errore="Il file deve avere estensione .csv")
    try:
        content = f.read(5 * 1024 * 1024)
        righe = _parse_csv_bancario(content)
    except ValueError as e:
        return render_template("costi_riconcilia.html", step=1, anno=anno, errore=str(e))

    uscite = [r for r in righe if r['tipo'] == 'uscita']
    if not uscite:
        return render_template("costi_riconcilia.html", step=1, anno=anno,
                               errore="Nessuna uscita trovata nel CSV.")

    conn = get_connection()
    auto_match, da_assegnare = _riconcilia_uscite(conn, uscite)
    conn.close()

    return render_template("costi_riconcilia.html", step=2, anno=anno,
        auto_match=auto_match,
        da_assegnare=da_assegnare,
        auto_match_json=json.dumps(auto_match),
        categorie=CATEGORIE_COSTI,
    )


@app.route("/costi-anno/riconcilia/conferma", methods=["POST"])
@login_required
def riconcilia_conferma():
    anno = int(request.form.get("anno", datetime.date.today().year))
    auto_match = json.loads(request.form.get("auto_match_json", "[]"))

    conn = get_connection()
    importati = 0
    abbinati = 0
    ignorati = 0

    def _inserisci_o_aggiungi(nome, categoria, data, importo, h, csv_desc=None):
        """Crea scadenza+rata oppure aggiunge rata a scadenza esistente con lo stesso nome.
        csv_desc = descrizione bancaria originale, salvata per il matching storico futuro."""
        sc_esistente = conn.execute("SELECT id FROM scadenze_costi WHERE nome=%s", (nome,)).fetchone()
        if sc_esistente:
            sid = sc_esistente['id']
        else:
            cur = conn.execute("""
                INSERT INTO scadenze_costi (nome, categoria, importo_rata, uscita_cassa_rata,
                    ricorrenza, data_prima_scadenza, num_rate)
                VALUES (%s, %s, 0, %s, 'mensile', %s, 1) RETURNING id
            """, (nome, categoria, importo, data))
            sid = cur.fetchone()[0]
        # Evita rata duplicata
        if conn.execute("""
            SELECT id FROM rate_scadenza_costo
            WHERE scadenza_costo_id=%s AND data_pagamento=%s
              AND ROUND(COALESCE(uscita_cassa,0)::numeric,2)=ROUND(%s::numeric,2)
        """, (sid, data, importo)).fetchone():
            return
        num = conn.execute(
            "SELECT COALESCE(MAX(numero_rata),0)+1 FROM rate_scadenza_costo WHERE scadenza_costo_id=%s",
            (sid,)
        ).fetchone()[0]
        conn.execute("""
            INSERT INTO rate_scadenza_costo
                (scadenza_costo_id, numero_rata, importo, uscita_cassa, data_scadenza,
                 pagato, data_pagamento, csv_hash, csv_descrizione)
            VALUES (%s, %s, 0, %s, %s, 1, %s, %s, %s)
        """, (sid, num, importo, data, data, h, csv_desc))
        _upsert_movimento(conn, data, nome, importo, h)

    # 1. Auto-match: aggiorna rate esistenti (salva anche csv_descrizione)
    for item in auto_match:
        rata = conn.execute("SELECT pagato FROM rate_scadenza_costo WHERE id=%s", (item['rata_id'],)).fetchone()
        if rata and rata['pagato']:
            conn.execute("""UPDATE rate_scadenza_costo
                SET uscita_cassa=%s, csv_hash=%s, csv_descrizione=%s WHERE id=%s""",
                (item['importo'], item['hash'], item['descrizione'], item['rata_id']))
        else:
            conn.execute("""
                UPDATE rate_scadenza_costo
                SET uscita_cassa=%s, pagato=1, data_pagamento=%s, csv_hash=%s, csv_descrizione=%s
                WHERE id=%s
            """, (item['importo'], item['data'], item['hash'], item['descrizione'], item['rata_id']))
        _upsert_movimento(conn, item['data'], item['descrizione'], item['importo'], item['hash'])
        abbinati += 1

    # 2. Da assegnare: ogni riga ha nome e categoria editati dall'utente
    idx = 0
    while True:
        h = request.form.get(f"da[{idx}][hash]")
        if h is None:
            break
        ignora = request.form.get(f"da[{idx}][ignora]") == "1"
        data = request.form.get(f"da[{idx}][data]")
        descrizione = request.form.get(f"da[{idx}][descrizione]", "")
        importo = float(request.form.get(f"da[{idx}][importo]", 0))
        nome = request.form.get(f"da[{idx}][nome]", descrizione[:60]).strip() or descrizione[:60]
        categoria = request.form.get(f"da[{idx}][categoria]", "altro")

        if ignora:
            ignorati += 1
        else:
            _inserisci_o_aggiungi(nome, categoria, data, importo, h, csv_desc=descrizione)
            # Salva regola per riconoscimento futuro
            _auto_salva_regola(conn, descrizione, nome, categoria)
            importati += 1
        idx += 1

    conn.commit()
    conn.close()
    return render_template("costi_riconcilia.html", step=3, anno=anno,
        abbinati=abbinati, importati=importati, ignorati=ignorati)


@app.route("/riconciliazione/regole")
@login_required
def riconciliazione_regole():
    conn = get_connection()
    regole = conn.execute("SELECT * FROM riconciliazione_regole ORDER BY id").fetchall()
    conn.close()
    return render_template("riconcilia_regole.html", regole=regole, categorie=CATEGORIE_COSTI)


@app.route("/riconciliazione/regola/nuova", methods=["POST"])
@login_required
def riconciliazione_regola_nuova():
    conn = get_connection()
    pattern = request.form["pattern"]
    nome_costo = request.form["nome_costo"]
    categoria = request.form.get("categoria", "altro")
    importo_esatto = request.form.get("importo_esatto", "").strip()
    importo_esatto = float(importo_esatto) if importo_esatto else None
    raggruppa = 1 if request.form.get("raggruppa_per_mese") else 0
    conn.execute("""
        INSERT INTO riconciliazione_regole (pattern, importo_esatto, nome_costo, categoria, raggruppa_per_mese)
        VALUES (%s, %s, %s, %s, %s)
    """, (pattern, importo_esatto, nome_costo, categoria, raggruppa))
    conn.commit()
    conn.close()
    return redirect(url_for("riconciliazione_regole"))


@app.route("/riconciliazione/regola/<int:rid>/elimina", methods=["POST"])
@login_required
def riconciliazione_regola_elimina(rid):
    conn = get_connection()
    conn.execute("DELETE FROM riconciliazione_regole WHERE id=%s", (rid,))
    conn.commit()
    conn.close()
    return redirect(url_for("riconciliazione_regole"))


# ─────────────────────────────────────────
# CFO VIRTUALE
# ─────────────────────────────────────────

def _build_cfo_data(conn, anno_ref=None):
    """Raccoglie e calcola tutti i KPI e proiezioni per il modulo CFO.

    anno_ref: anno di riferimento (int). Se None o anno corrente → usa oggi.
              Se anno passato → imposta today = 31 dic di quell'anno, tutti i
              calcoli (burn rate, trend, saldo, ecc.) usano quella data come
              punto di riferimento.
    """
    actual_today = datetime.date.today()
    anno_corrente_reale = actual_today.year
    if anno_ref and anno_ref < anno_corrente_reale:
        today = datetime.date(anno_ref, 12, 31)
    else:
        anno_ref = anno_corrente_reale
        today = actual_today
    c = conn.cursor()

    # --- Saldo al giorno di riferimento (base fissa 5142.27 al 01/01/2026) ---
    SALDO_DATA_INIZIO = '2026-01-01'
    row = c.execute("SELECT valore FROM impostazioni WHERE chiave='saldo_iniziale_conto'").fetchone()
    saldo_iniziale = float(row[0]) if row else 0.0
    totale_entrate_all = c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='entrata' AND data >= %s AND data <= %s",
        (SALDO_DATA_INIZIO, today.isoformat())
    ).fetchone()[0]
    totale_uscite_all = c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='uscita' AND data >= %s AND data <= %s",
        (SALDO_DATA_INIZIO, today.isoformat())
    ).fetchone()[0]
    saldo_attuale = saldo_iniziale + totale_entrate_all - totale_uscite_all

    # --- MRR / ARR (solo abbonamenti attivi con rate future) ---
    abbonamenti = c.execute("""
        SELECT ct.percentuale_partner, AVG(rc.importo) as rata_mensile
        FROM contratti ct
        JOIN rate_contratto rc ON rc.contratto_id = ct.id
        WHERE ct.tipo_pagamento = 'abbonamento' AND ct.stato = 'attivo'
          AND rc.pagato = 0 AND rc.data_scadenza >= %s
        GROUP BY ct.id, ct.percentuale_partner
    """, (today.isoformat(),)).fetchall()
    mrr = sum(float(a['rata_mensile']) for a in abbonamenti)
    mrr_netto = sum(float(a['rata_mensile']) * (1 - (float(a['percentuale_partner']) or 0) / 100) for a in abbonamenti)
    arr = mrr * 12

    # --- Burn rate e entrate (ultimi 3 mesi) ---
    tre_mesi_fa = _add_months(today, -3).isoformat()
    sei_mesi_fa = _add_months(today, -6).isoformat()

    entrate_3m = c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='entrata' AND data >= %s",
        (tre_mesi_fa,)
    ).fetchone()[0]
    uscite_3m = c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='uscita' AND data >= %s",
        (tre_mesi_fa,)
    ).fetchone()[0]
    burn_rate = uscite_3m / 3.0 if uscite_3m > 0 else 0.0

    entrate_prec_3m = c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='entrata' AND data >= %s AND data < %s",
        (sei_mesi_fa, tre_mesi_fa)
    ).fetchone()[0]
    revenue_trend_pct = ((entrate_3m - entrate_prec_3m) / entrate_prec_3m * 100) if entrate_prec_3m > 0 else 0.0

    # --- Runway ---
    runway_mesi = (saldo_attuale / burn_rate) if burn_rate > 0 else 99.0
    runway_mesi = max(runway_mesi, 0.0)

    # --- Margine operativo anno corrente ---
    anno_corrente = str(anno_ref)
    entrate_anno = c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='entrata' AND LEFT(data, 4)=%s",
        (anno_corrente,)
    ).fetchone()[0]
    uscite_anno = c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='uscita' AND LEFT(data, 4)=%s",
        (anno_corrente,)
    ).fetchone()[0]
    margine_pct = ((entrate_anno - uscite_anno) / entrate_anno * 100) if entrate_anno > 0 else 0.0

    # --- Pipeline ---
    pipeline_rows = c.execute("""
        SELECT stato, COALESCE(SUM(valore_stimato),0) as valore, COUNT(*) as cnt
        FROM opportunita GROUP BY stato
    """).fetchall()
    pipeline_aperta   = sum(float(p['valore']) for p in pipeline_rows if p['stato'] not in ('firmato', 'perso'))
    pipeline_firmata  = sum(float(p['valore']) for p in pipeline_rows if p['stato'] == 'firmato')
    totale_opps       = c.execute("SELECT COUNT(*) FROM opportunita").fetchone()[0]
    firmate           = c.execute("SELECT COUNT(*) FROM opportunita WHERE stato='firmato'").fetchone()[0]
    conversion_rate   = (firmate / totale_opps * 100) if totale_opps > 0 else 0.0

    # --- Rate contratti future (per proiezione) ---
    rate_future = c.execute("""
        SELECT rc.data_scadenza, rc.importo, ct.percentuale_partner
        FROM rate_contratto rc
        JOIN contratti ct ON rc.contratto_id = ct.id
        WHERE rc.pagato = 0 AND rc.data_scadenza >= %s
        ORDER BY rc.data_scadenza
    """, (today.isoformat(),)).fetchall()

    # --- Costi futuri pianificati (scadenze_costi) ---
    costi_futuri = c.execute("""
        SELECT rsc.data_scadenza, rsc.importo
        FROM rate_scadenza_costo rsc
        WHERE rsc.pagato = 0 AND rsc.data_scadenza >= %s
        ORDER BY rsc.data_scadenza
    """, (today.isoformat(),)).fetchall()

    # --- Cash Flow Anno Corrente (Gennaio – Dicembre) ---
    anno_corrente = today.year

    # Dati reali da movimenti per ogni mese dell'anno corrente
    mov_rows = c.execute("""
        SELECT
            CAST(SUBSTRING(data FROM 6 FOR 2) AS INTEGER) as mese,
            tipo,
            COALESCE(SUM(importo), 0) as totale
        FROM movimenti
        WHERE LEFT(data, 4) = %s
        GROUP BY mese, tipo
    """, (str(anno_corrente),)).fetchall()
    movimenti_per_mese = {}
    for r in mov_rows:
        mn = r['mese']
        if mn not in movimenti_per_mese:
            movimenti_per_mese[mn] = {'entrate': 0.0, 'uscite': 0.0}
        if r['tipo'] == 'entrata':
            movimenti_per_mese[mn]['entrate'] = float(r['totale'])
        else:
            movimenti_per_mese[mn]['uscite'] = float(r['totale'])

    # Saldo al 1° Gennaio: saldo_attuale meno la variazione netta dell'anno fino ad oggi
    entrate_ytd = sum(v['entrate'] for v in movimenti_per_mese.values())
    uscite_ytd  = sum(v['uscite']  for v in movimenti_per_mese.values())
    saldo_inizio_anno = saldo_attuale - entrate_ytd + uscite_ytd

    cashflow_proj = []
    saldo_run = saldo_inizio_anno
    mesi_futuri_rimasti = 12 - today.month  # quanti mesi futuri restano dopo il corrente

    for mese_n in range(1, 13):
        mese_key = f"{anno_corrente}-{mese_n:02d}"
        mese_nome = MESI_IT[mese_n] + f" {anno_corrente}"

        if mese_n < today.month:
            # Mese già concluso: dati reali da movimenti (lordo IVA inclusa, partner già nelle uscite)
            dati = movimenti_per_mese.get(mese_n, {'entrate': 0.0, 'uscite': 0.0})
            entrate_m        = dati['entrate']   # lordo IVA inclusa
            uscite_m         = dati['uscite']    # tutte le uscite reali (inclusi partner)
            quota_partner_m  = None              # già inclusa nelle uscite reali
            is_actual        = True
            pipeline_bonus   = 0.0

        elif mese_n == today.month:
            # Mese corrente: dati reali registrati finora
            dati = movimenti_per_mese.get(mese_n, {'entrate': 0.0, 'uscite': 0.0})
            entrate_m        = dati['entrate']
            uscite_m         = dati['uscite']
            quota_partner_m  = None
            is_actual        = None
            pipeline_bonus   = 0.0

        else:
            # Mese futuro: entrate da rate contratti (IVA ESCLUSA, come inserite nei contratti)
            # Partner NON detratto dalle entrate — sarà pagato come uscita separata
            rate_mese = [r for r in rate_future if r['data_scadenza'] and r['data_scadenza'][:7] == mese_key]
            entrate_m       = sum(float(r['importo']) for r in rate_mese)
            quota_partner_m = sum(
                float(r['importo']) * (float(r['percentuale_partner']) or 0) / 100
                for r in rate_mese
            )
            uscite_m = sum(
                float(r['importo'])
                for r in costi_futuri
                if r['data_scadenza'] and r['data_scadenza'][:7] == mese_key
            )
            mesi_pip = min(mesi_futuri_rimasti, 6)
            pipeline_bonus = (pipeline_aperta * 0.25) / mesi_pip if mesi_pip > 0 and (mese_n - today.month) <= mesi_pip else 0.0
            is_actual = False

        # Netto cassa: per i mesi futuri detraggo quota partner e costi fissi dalle entrate nette
        partner_out     = quota_partner_m if quota_partner_m is not None else 0.0
        netto_base      = entrate_m - partner_out - uscite_m
        netto_ottimistico = entrate_m - partner_out + pipeline_bonus - uscite_m
        saldo_run      += netto_base

        cashflow_proj.append({
            'mese': mese_nome,
            'mese_key': mese_key,
            'entrate': entrate_m,
            'quota_partner': quota_partner_m,   # None=reale, float=stimata
            'uscite': uscite_m,
            'netto_base': netto_base,
            'netto_ottimistico': netto_ottimistico,
            'saldo_cumulativo': saldo_run,
            'negativo': netto_base < 0,
            'is_actual': is_actual,
        })

    # --- Business Health Score ---
    score = 0
    score_details = []

    # 1. Revenue trend (25 pts)
    if revenue_trend_pct > 10:
        pts = 25; tag = "ottimo"
    elif revenue_trend_pct > 0:
        pts = 17; tag = "buono"
    elif revenue_trend_pct > -10:
        pts = 8;  tag = "attenzione"
    else:
        pts = 0;  tag = "critico"
    score += pts
    score_details.append({"label": "Trend ricavi", "pts": pts, "max": 25, "tag": tag,
                          "valore": f"{revenue_trend_pct:+.1f}%"})

    # 2. Cash runway (25 pts)
    if runway_mesi > 12:
        pts = 25; tag = "ottimo"
    elif runway_mesi > 6:
        pts = 18; tag = "buono"
    elif runway_mesi > 3:
        pts = 10; tag = "attenzione"
    else:
        pts = 0;  tag = "critico"
    score += pts
    run_label = f"{runway_mesi:.0f} mesi" if runway_mesi < 99 else "∞"
    score_details.append({"label": "Cash Runway", "pts": pts, "max": 25, "tag": tag,
                          "valore": run_label})

    # 3. Margine operativo (25 pts)
    if margine_pct > 30:
        pts = 25; tag = "ottimo"
    elif margine_pct > 10:
        pts = 17; tag = "buono"
    elif margine_pct > 0:
        pts = 8;  tag = "attenzione"
    else:
        pts = 0;  tag = "critico"
    score += pts
    score_details.append({"label": "Margine operativo", "pts": pts, "max": 25, "tag": tag,
                          "valore": f"{margine_pct:.1f}%"})

    # 4. Pipeline coverage (25 pts) — pipeline vs 6 mesi di burn
    six_month_burn = burn_rate * 6
    pipeline_coverage = (pipeline_aperta / six_month_burn) if six_month_burn > 0 else (1.5 if pipeline_aperta > 0 else 0)
    if pipeline_coverage > 2:
        pts = 25; tag = "ottimo"
    elif pipeline_coverage > 1:
        pts = 17; tag = "buono"
    elif pipeline_coverage > 0.5:
        pts = 8;  tag = "attenzione"
    else:
        pts = 0;  tag = "critico"
    score += pts
    score_details.append({"label": "Pipeline Coverage", "pts": pts, "max": 25, "tag": tag,
                          "valore": f"{pipeline_coverage:.1f}x"})

    health_score = min(score, 100)

    # --- Smart Alerts ---
    alerts = []

    mesi_negativi = [p for p in cashflow_proj if p['negativo']]
    if mesi_negativi:
        elenco = ", ".join(p['mese'] for p in mesi_negativi[:3])
        alerts.append({
            'tipo': 'danger',
            'icona': '⚠️',
            'titolo': f"Cash flow negativo in {len(mesi_negativi)} {'mese' if len(mesi_negativi)==1 else 'mesi'}",
            'dettaglio': f"Mesi critici: {elenco}{'...' if len(mesi_negativi)>3 else ''}",
        })

    if runway_mesi < 3:
        alerts.append({
            'tipo': 'danger', 'icona': '🚨',
            'titolo': f"Runway critico: solo {runway_mesi:.1f} mesi di liquidità",
            'dettaglio': "Cerca nuovi contratti o riduci i costi fissi con urgenza.",
        })
    elif runway_mesi < 6:
        alerts.append({
            'tipo': 'warning', 'icona': '⚡',
            'titolo': f"Runway limitato: {runway_mesi:.1f} mesi di liquidità",
            'dettaglio': "Accelera la pipeline o rinegozia i costi fissi.",
        })

    in_90 = (today + datetime.timedelta(days=90)).isoformat()
    contratti_scad = c.execute("""
        SELECT ct.titolo, ct.importo_totale, cl.nome as cliente_nome
        FROM contratti ct JOIN clienti cl ON ct.cliente_id = cl.id
        WHERE ct.data_fine BETWEEN %s AND %s AND ct.stato = 'attivo'
    """, (today.isoformat(), in_90)).fetchall()
    if contratti_scad:
        val_rischio = sum(float(ct['importo_totale']) for ct in contratti_scad)
        alerts.append({
            'tipo': 'warning', 'icona': '📋',
            'titolo': f"{len(contratti_scad)} contratto/i in scadenza entro 90 giorni",
            'dettaglio': f"Valore a rischio: €{val_rischio:,.0f}. Avvia subito le rinegoziazioni.",
        })

    gg60_fa = (today - datetime.timedelta(days=60)).isoformat()
    opps_stagnanti = c.execute("""
        SELECT COUNT(*) FROM opportunita
        WHERE stato NOT IN ('firmato','perso') AND data_creazione <= %s
    """, (gg60_fa,)).fetchone()[0]
    if opps_stagnanti > 0:
        alerts.append({
            'tipo': 'info', 'icona': '📊',
            'titolo': f"{opps_stagnanti} opportunità ferme in pipeline da oltre 60 giorni",
            'dettaglio': "Fai follow-up o chiudi le opportunità inattive per mantenere la pipeline pulita.",
        })

    if revenue_trend_pct < -15:
        alerts.append({
            'tipo': 'danger', 'icona': '📉',
            'titolo': f"Calo ricavi del {abs(revenue_trend_pct):.0f}% rispetto al trimestre precedente",
            'dettaglio': "Analizza i clienti persi e intensifica le attività commerciali.",
        })

    if not alerts:
        alerts.append({
            'tipo': 'success', 'icona': '✅',
            'titolo': "Tutto OK — nessun problema rilevato",
            'dettaglio': "La tua azienda è in buona salute finanziaria. Continua così!",
        })

    # --- Contratti attivi e clienti ---
    num_contratti_attivi = c.execute("SELECT COUNT(*) FROM contratti WHERE stato='attivo'").fetchone()[0]
    num_clienti_attivi   = c.execute("SELECT COUNT(DISTINCT cliente_id) FROM contratti WHERE stato='attivo'").fetchone()[0]

    # --- Budget vs Consuntivo ---
    bva_rows = c.execute("""
        SELECT mese, categoria, tipo, importo as budget
        FROM budget WHERE anno = %s ORDER BY mese, tipo, categoria
    """, (anno_corrente,)).fetchall()

    actual_cat_rows = c.execute("""
        SELECT CAST(SUBSTRING(data FROM 6 FOR 2) AS INTEGER) as mese,
               COALESCE(categoria, 'altro') as categoria, tipo,
               SUM(importo) as totale
        FROM movimenti WHERE LEFT(data, 4) = %s
        GROUP BY mese, categoria, tipo
    """, (str(anno_corrente),)).fetchall()
    actual_map = {(r['mese'], r['categoria'], r['tipo']): float(r['totale']) for r in actual_cat_rows}

    # Riepilogo mensile budget vs actual (per grafico)
    budget_mensile = []
    for mn in range(1, min(today.month + 1, 13)):
        b_ent = sum(float(r['budget']) for r in bva_rows if r['mese'] == mn and r['tipo'] == 'entrata')
        b_usc = sum(float(r['budget']) for r in bva_rows if r['mese'] == mn and r['tipo'] == 'uscita')
        a_ent = sum(v for (m, _, t), v in actual_map.items() if m == mn and t == 'entrata')
        a_usc = sum(v for (m, _, t), v in actual_map.items() if m == mn and t == 'uscita')
        budget_mensile.append({
            'mese': MESI_IT[mn], 'mese_n': mn,
            'budget_entrate': b_ent, 'actual_entrate': a_ent,
            'budget_uscite': b_usc,  'actual_uscite': a_usc,
            'var_entrate': a_ent - b_ent,
            'var_uscite':  a_usc - b_usc,
        })

    # Dettaglio mese corrente per categoria
    bva_corrente = []
    cats_viste = set()
    for r in bva_rows:
        if r['mese'] == today.month:
            k = (r['tipo'], r['categoria'])
            if k in cats_viste:
                continue
            cats_viste.add(k)
            bgt = float(r['budget'])
            act = actual_map.get((today.month, r['categoria'], r['tipo']), 0.0)
            var = act - bgt
            bva_corrente.append({
                'tipo': r['tipo'], 'categoria': r['categoria'],
                'budget': bgt, 'actual': act,
                'varianza': var,
                'varianza_pct': (var / bgt * 100) if bgt != 0 else 0,
            })
    bva_corrente.sort(key=lambda x: (x['tipo'], -abs(x['varianza'])))

    # --- Crediti Scaduti & Aging ---
    crediti_rows = c.execute("""
        SELECT rc.id, rc.importo, rc.data_scadenza,
               ct.titolo, cl.nome as cliente_nome, ct.id as contratto_id,
               (CURRENT_DATE - rc.data_scadenza::date) AS giorni_scaduto
        FROM rate_contratto rc
        JOIN contratti ct ON rc.contratto_id = ct.id
        JOIN clienti cl ON ct.cliente_id = cl.id
        WHERE rc.pagato = 0
        ORDER BY rc.data_scadenza
    """).fetchall()

    totale_crediti  = sum(float(r['importo']) for r in crediti_rows)
    crediti_scaduti = [r for r in crediti_rows if r['giorni_scaduto'] > 0]
    totale_scaduto  = sum(float(r['importo']) for r in crediti_scaduti)
    aging_0_30   = sum(float(r['importo']) for r in crediti_scaduti if r['giorni_scaduto'] <= 30)
    aging_31_60  = sum(float(r['importo']) for r in crediti_scaduti if 30 < r['giorni_scaduto'] <= 60)
    aging_60plus = sum(float(r['importo']) for r in crediti_scaduti if r['giorni_scaduto'] > 60)

    # Aggrega per cliente
    crediti_per_cl = {}
    for r in crediti_scaduti:
        cl = r['cliente_nome']
        if cl not in crediti_per_cl:
            crediti_per_cl[cl] = {'importo': 0.0, 'rate': 0, 'max_giorni': 0, 'contratto_id': r['contratto_id']}
        crediti_per_cl[cl]['importo']   += float(r['importo'])
        crediti_per_cl[cl]['rate']      += 1
        crediti_per_cl[cl]['max_giorni'] = max(crediti_per_cl[cl]['max_giorni'], r['giorni_scaduto'])
    top_debitori = sorted(
        [{'cliente': n, **d} for n, d in crediti_per_cl.items()],
        key=lambda x: x['importo'], reverse=True
    )[:6]

    # --- Qualità del Fatturato ---
    ric_ricorrenti = c.execute("""
        SELECT COALESCE(SUM(rc.importo), 0)
        FROM rate_contratto rc JOIN contratti ct ON rc.contratto_id = ct.id
        WHERE ct.tipo_pagamento = 'abbonamento' AND rc.pagato = 1
          AND LEFT(rc.data_pagamento, 4) = %s
    """, (str(anno_corrente),)).fetchone()[0]

    ric_unatantum = c.execute("""
        SELECT COALESCE(SUM(rc.importo), 0)
        FROM rate_contratto rc JOIN contratti ct ON rc.contratto_id = ct.id
        WHERE ct.tipo_pagamento != 'abbonamento' AND rc.pagato = 1
          AND LEFT(rc.data_pagamento, 4) = %s
    """, (str(anno_corrente),)).fetchone()[0]

    ric_ricorrenti = float(ric_ricorrenti)
    ric_unatantum  = float(ric_unatantum)
    tot_ric_anno   = ric_ricorrenti + ric_unatantum
    pct_ricorrente = (ric_ricorrenti / tot_ric_anno * 100) if tot_ric_anno > 0 else 0.0

    # --- Concentrazione Clienti ---
    conc_rows = c.execute("""
        SELECT cl.nome as cliente, cl.id as cliente_id,
               COALESCE(SUM(rc.importo), 0) as fatturato
        FROM rate_contratto rc
        JOIN contratti ct ON rc.contratto_id = ct.id
        JOIN clienti cl ON ct.cliente_id = cl.id
        WHERE rc.pagato = 1 AND LEFT(rc.data_pagamento, 4) = %s
        GROUP BY ct.cliente_id, cl.nome, cl.id ORDER BY fatturato DESC
    """, (str(anno_corrente),)).fetchall()

    tot_fatt_anno = sum(float(r['fatturato']) for r in conc_rows)
    concentrazione = []
    cumulo = 0.0
    for r in conc_rows:
        f = float(r['fatturato'])
        cumulo += f
        concentrazione.append({
            'cliente': r['cliente'],
            'cliente_id': r['cliente_id'],
            'fatturato': f,
            'pct': (f / tot_fatt_anno * 100) if tot_fatt_anno > 0 else 0.0,
            'cumulo_pct': (cumulo / tot_fatt_anno * 100) if tot_fatt_anno > 0 else 0.0,
        })
    top3_pct = concentrazione[2]['cumulo_pct'] if len(concentrazione) >= 3 else (concentrazione[-1]['cumulo_pct'] if concentrazione else 0.0)

    # --- Piano d'Azione ---
    azioni = []

    if crediti_scaduti:
        nomi_top = ', '.join(d['cliente'] for d in top_debitori[:2])
        azioni.append({
            'priorita': 'alta', 'icona': '💌', 'area': 'Crediti',
            'titolo': f"Sollecita {len(crediti_scaduti)} rata/e scaduta/e — €{totale_scaduto:,.0f}",
            'dettaglio': f"Clienti con maggiore ritardo: {nomi_top}.",
        })

    mesi_neg_vicini = [p for i, p in enumerate(cashflow_proj) if p['is_actual'] == False and p['negativo'] and i < 6]
    if mesi_neg_vicini:
        p0 = mesi_neg_vicini[0]
        azioni.append({
            'priorita': 'alta', 'icona': '⚡', 'area': 'Cash Flow',
            'titolo': f"Cash flow negativo a {p0['mese']} (€{p0['netto_base']:,.0f})",
            'dettaglio': "Anticipa fatturazione o porta avanti opportunità dalla pipeline per coprire il mese.",
        })

    contratti_scad_90 = c.execute("""
        SELECT ct.titolo, cl.nome as cliente_nome, ct.data_fine
        FROM contratti ct JOIN clienti cl ON ct.cliente_id = cl.id
        WHERE ct.data_fine BETWEEN %s AND %s AND ct.stato = 'attivo'
        ORDER BY ct.data_fine
    """, (today.isoformat(), (today + datetime.timedelta(days=90)).isoformat())).fetchall()
    if contratti_scad_90:
        azioni.append({
            'priorita': 'alta', 'icona': '📋', 'area': 'Contratti',
            'titolo': f"Rinnova {len(contratti_scad_90)} contratto/i in scadenza (90gg)",
            'dettaglio': ', '.join(f"{ct['cliente_nome']} — {ct['data_fine']}" for ct in contratti_scad_90[:3]),
        })

    gg60_fa = (today - datetime.timedelta(days=60)).isoformat()
    opps_stag = c.execute("""
        SELECT nome_azienda, valore_stimato FROM opportunita
        WHERE stato NOT IN ('firmato','perso') AND data_creazione <= %s
        ORDER BY valore_stimato DESC LIMIT 4
    """, (gg60_fa,)).fetchall()
    if opps_stag:
        tot_stag = sum(float(o['valore_stimato']) for o in opps_stag)
        azioni.append({
            'priorita': 'media', 'icona': '🚀', 'area': 'Pipeline',
            'titolo': f"Riattiva {len(opps_stag)} opportunità ferme 60+ giorni — €{tot_stag:,.0f}",
            'dettaglio': ', '.join(o['nome_azienda'] for o in opps_stag),
        })

    if top3_pct > 60 and len(concentrazione) >= 3:
        azioni.append({
            'priorita': 'media', 'icona': '⚠️', 'area': 'Strategia',
            'titolo': f"Rischio concentrazione: top 3 clienti = {top3_pct:.0f}% del fatturato",
            'dettaglio': "Diversifica la base clienti per ridurre la dipendenza da pochi account.",
        })

    over_budget = [b for b in bva_corrente if b['tipo'] == 'uscita' and b['varianza'] > 0 and b['varianza_pct'] > 20]
    if over_budget:
        peggio = sorted(over_budget, key=lambda x: x['varianza'], reverse=True)[0]
        azioni.append({
            'priorita': 'media', 'icona': '📊', 'area': 'Budget',
            'titolo': f"Sforamento budget: {peggio['categoria'].capitalize()} +€{peggio['varianza']:,.0f} ({peggio['varianza_pct']:+.0f}%)",
            'dettaglio': "Verifica le spese in questa categoria e aggiorna il budget se necessario.",
        })

    if runway_mesi < 6:
        azioni.append({
            'priorita': 'alta' if runway_mesi < 3 else 'media', 'icona': '🏦', 'area': 'Liquidità',
            'titolo': f"Runway {runway_mesi:.1f} mesi — valuta linea di credito o anticipa incassi",
            'dettaglio': "Con il burn rate attuale la liquidità si esaurisce presto.",
        })

    if pct_ricorrente < 40 and tot_ric_anno > 0:
        azioni.append({
            'priorita': 'media', 'icona': '🔄', 'area': 'Revenue',
            'titolo': f"Solo il {pct_ricorrente:.0f}% del fatturato è ricorrente",
            'dettaglio': "Converti più clienti a contratti ricorrenti (abbonamento) per stabilizzare i ricavi.",
        })

    if not azioni:
        azioni.append({
            'priorita': 'info', 'icona': '✅', 'area': 'Generale',
            'titolo': "Nessuna azione urgente — situazione sotto controllo",
            'dettaglio': "Continua a monitorare pipeline, rinnovi e scadenze.",
        })

    azioni.sort(key=lambda x: 0 if x['priorita'] == 'alta' else (1 if x['priorita'] == 'media' else 2))

    # --- P&L da costi_contabili (Conto Economico Profis) ---
    # Recupera tutti i mesi disponibili dell'anno corrente
    mesi_contabili = c.execute("""
        SELECT DISTINCT anno, mese FROM costi_contabili
        WHERE anno = %s ORDER BY mese
    """, (anno_corrente,)).fetchall()

    pl_mesi = []
    for row in mesi_contabili:
        a_pl, m_pl = row['anno'], row['mese']
        # Costi livello 0 (categorie principali)
        costi_rows = c.execute("""
            SELECT conto, descrizione, saldo_finale
            FROM costi_contabili
            WHERE anno=%s AND mese=%s AND livello=0
            ORDER BY conto
        """, (a_pl, m_pl)).fetchall()

        totale_costi = sum(float(r['saldo_finale']) for r in costi_rows)
        ammortamenti = sum(float(r['saldo_finale']) for r in costi_rows if r['conto'].startswith('80'))
        oneri_fin    = sum(float(r['saldo_finale']) for r in costi_rows if r['conto'].startswith('75'))

        # Ricavi da movimenti del mese (lordo IVA inclusa → netto stimato /1.22)
        ent_reale = float(movimenti_per_mese.get(m_pl, {}).get('entrate', 0.0))
        ricavi_netti = ent_reale / 1.22

        risultato  = ricavi_netti - totale_costi
        ebitda     = ricavi_netti - (totale_costi - ammortamenti - oneri_fin)

        pl_mesi.append({
            'mese': MESI_IT[m_pl] + f" {a_pl}",
            'mese_n': m_pl,
            'ricavi_netti': ricavi_netti,
            'totale_costi': totale_costi,
            'ammortamenti': ammortamenti,
            'oneri_fin': oneri_fin,
            'ebitda': ebitda,
            'risultato': risultato,
            'costi_dettaglio': [dict(r) for r in costi_rows],
        })

    # ── Revenue Engine ──────────────────────────────────────────────────
    anno_fa_str = (today - datetime.timedelta(days=365)).isoformat()
    contratti_chiusi_anno = c.execute("""
        SELECT COUNT(*) FROM contratti
        WHERE stato != 'attivo'
          AND data_fine IS NOT NULL AND data_fine != ''
          AND data_fine >= %s AND data_fine <= %s
    """, (anno_fa_str, today.isoformat())).fetchone()[0]
    tot_base_churn = num_contratti_attivi + contratti_chiusi_anno
    churn_rate_pct    = round(contratti_chiusi_anno / tot_base_churn * 100.0, 1) if tot_base_churn > 0 else 0.0
    renewal_rate_pct  = round(100.0 - churn_rate_pct, 1)

    dur_row = c.execute("""
        SELECT AVG(
            (COALESCE(NULLIF(data_fine,''), CURRENT_DATE::text)::date - data_inizio::date)::float / 30.44
        )
        FROM contratti
        WHERE data_inizio IS NOT NULL AND data_inizio != ''
    """).fetchone()[0]
    durata_media_mesi = round(float(dur_row), 1) if dur_row else 0.0

    servizi_rows = c.execute("""
        SELECT servizio, COUNT(*) as num,
               AVG(importo_totale) as ticket_medio,
               SUM(importo_totale) as totale
        FROM contratti
        WHERE stato = 'attivo' AND servizio IS NOT NULL AND servizio != ''
        GROUP BY servizio ORDER BY totale DESC
    """).fetchall()
    ticket_per_servizio = [
        {'servizio': r['servizio'], 'num': r['num'],
         'ticket_medio': round(float(r['ticket_medio']), 0),
         'totale': round(float(r['totale']), 0)}
        for r in servizi_rows
    ]

    stagionalita = []
    for i in range(11, -1, -1):
        y_st, m_st = today.year, today.month - i
        while m_st <= 0:
            m_st += 12; y_st -= 1
        ent_s = c.execute(
            "SELECT COALESCE(SUM(importo),0) FROM movimenti WHERE tipo='entrata' AND LEFT(data, 7)=%s",
            (f"{y_st:04d}-{m_st:02d}",)
        ).fetchone()[0]
        stagionalita.append({'mese': MESI_IT[m_st][:3] + f" {str(y_st)[2:]}", 'entrate': float(ent_s)})

    # ── Indicatori di Efficienza ─────────────────────────────────────────
    dso_giorni = round(totale_crediti / entrate_3m * 90.0, 1) if entrate_3m > 0 else 0.0

    rit_row = c.execute("""
        SELECT AVG((data_pagamento::date - data_scadenza::date)::float)
        FROM rate_contratto
        WHERE pagato = 1
          AND data_pagamento IS NOT NULL AND data_scadenza IS NOT NULL
          AND data_pagamento != '' AND data_scadenza != ''
          AND data_pagamento > data_scadenza
    """).fetchone()[0]
    ritardo_medio_gg = round(float(rit_row), 1) if rit_row else 0.0

    emesso_anno = float(c.execute(
        "SELECT COALESCE(SUM(importo),0) FROM rate_contratto WHERE LEFT(data_scadenza, 4)=%s",
        (str(anno_corrente),)
    ).fetchone()[0])
    incassato_anno_val    = ric_ricorrenti + ric_unatantum
    cash_conversion_ratio = round(incassato_anno_val / emesso_anno * 100.0, 1) if emesso_anno > 0 else 0.0

    num_cl           = max(num_clienti_attivi, 1)
    mrr_per_cliente  = round(mrr_netto / num_cl, 0) if mrr_netto > 0 else 0.0
    churn_mens_frac  = (churn_rate_pct / 100.0) / 12.0
    if churn_mens_frac > 0:
        ltv_stimato = round(mrr_per_cliente / churn_mens_frac, 0)
    elif durata_media_mesi > 0:
        ltv_stimato = round(mrr_per_cliente * durata_media_mesi, 0)
    else:
        ltv_stimato = round(mrr_per_cliente * 24.0, 0)

    return {
        'saldo_attuale': saldo_attuale,
        'mrr': mrr,
        'mrr_netto': mrr_netto,
        'arr': arr,
        'burn_rate': burn_rate,
        'runway_mesi': runway_mesi,
        'revenue_trend_pct': revenue_trend_pct,
        'entrate_3m': entrate_3m,
        'uscite_3m': uscite_3m,
        'margine_pct': margine_pct,
        'pipeline_aperta': pipeline_aperta,
        'pipeline_firmata': pipeline_firmata,
        'conversion_rate': conversion_rate,
        'cashflow_proj': cashflow_proj,
        'health_score': health_score,
        'score_details': score_details,
        'alerts': alerts,
        'num_contratti_attivi': num_contratti_attivi,
        'num_clienti_attivi': num_clienti_attivi,
        'pl_mesi': pl_mesi,
        # Budget vs Consuntivo
        'budget_mensile': budget_mensile,
        'bva_corrente': bva_corrente,
        # Crediti aging
        'totale_crediti': totale_crediti,
        'totale_scaduto': totale_scaduto,
        'aging_0_30': aging_0_30,
        'aging_31_60': aging_31_60,
        'aging_60plus': aging_60plus,
        'top_debitori': top_debitori,
        'n_crediti_scaduti': len(crediti_scaduti),
        # Qualità fatturato
        'ric_ricorrenti': ric_ricorrenti,
        'ric_unatantum': ric_unatantum,
        'pct_ricorrente': pct_ricorrente,
        'tot_ric_anno': tot_ric_anno,
        # Concentrazione clienti
        'concentrazione': concentrazione,
        'top3_pct': top3_pct,
        # Piano d'azione
        'azioni': azioni,
        # Revenue Engine
        'churn_rate_pct': churn_rate_pct,
        'renewal_rate_pct': renewal_rate_pct,
        'durata_media_mesi': durata_media_mesi,
        'ticket_per_servizio': ticket_per_servizio,
        'stagionalita': stagionalita,
        # Efficienza
        'dso_giorni': dso_giorni,
        'ritardo_medio_gg': ritardo_medio_gg,
        'cash_conversion_ratio': cash_conversion_ratio,
        'emesso_anno': emesso_anno,
        'incassato_anno_val': incassato_anno_val,
        'ltv_stimato': ltv_stimato,
        'mrr_per_cliente': mrr_per_cliente,
        'today': today.isoformat(),
        # Weekly KPI
        'contratti_chiusi_settimana': c.execute("""
            SELECT COUNT(*) FROM contratti
            WHERE stato='attivo'
              AND data_firma IS NOT NULL
              AND data_firma >= %s
              AND data_firma <= %s
        """, ((today - datetime.timedelta(days=7)).isoformat(), today.isoformat())).fetchone()[0],
        'fatturato_mese': c.execute("""
            SELECT COALESCE(SUM(importo),0) FROM rate_contratto
            WHERE fatturato = 1
              AND LEFT(data_fatturazione, 7) = LEFT(%s, 7)
        """, (today.isoformat(),)).fetchone()[0],
    }


# ─────────────────────────────────────────
# P&L — Conto Economico (dati Profis + ricavi da movimenti)
# ─────────────────────────────────────────

@app.route("/pl")
@login_required
def pl():
    anno = int(request.args.get('anno', datetime.date.today().year))
    conn = get_connection()

    # Mesi con dati costi importati da Profis
    mesi_contabili = conn.execute("""
        SELECT DISTINCT anno, mese FROM costi_contabili
        WHERE anno = %s ORDER BY mese
    """, (str(anno),)).fetchall()
    mesi_con_dati = {r['mese'] for r in mesi_contabili}

    # Costi per mese da Profis — totali (saldo, non_rett, rettifiche) livello 0
    costi_per_mese = {}
    conti_tutti = {}
    for row in mesi_contabili:
        m = row['mese']
        righe = conn.execute("""
            SELECT conto, descrizione, saldo_finale
            FROM costi_contabili WHERE anno=%s AND mese=%s AND livello=0 ORDER BY conto
        """, (row['anno'], m)).fetchall()
        costi_per_mese[m] = {}
        for r in righe:
            costi_per_mese[m][r['conto']] = float(r['saldo_finale'])
            if r['conto'] not in conti_tutti:
                conti_tutti[r['conto']] = r['descrizione']

        tot = conn.execute("""
            SELECT COALESCE(SUM(saldo_non_rettificato),0) as nr,
                   COALESCE(SUM(rettifiche),0) as rt,
                   COALESCE(SUM(saldo_finale),0) as sf
            FROM costi_contabili WHERE anno=%s AND mese=%s AND livello=0
        """, (str(anno), m)).fetchone()
        costi_per_mese[m]['_totali'] = {
            'non_rett': float(tot['nr']),
            'rettifiche': float(tot['rt']),
            'saldo': float(tot['sf']),
        }

    conti_ordinati = sorted(k for k in conti_tutti.keys())

    # Ricavi da Profis per mese — totali e per conto (livello 0)
    mesi_ricavi_profis = conn.execute("""
        SELECT DISTINCT anno, mese FROM ricavi_contabili WHERE anno=%s ORDER BY mese
    """, (str(anno),)).fetchall()
    mesi_con_ricavi_profis = {r['mese'] for r in mesi_ricavi_profis}

    ricavi_profis_per_mese = {}
    conti_ricavi_tutti = {}
    for row in mesi_ricavi_profis:
        m = row['mese']
        righe = conn.execute("""
            SELECT conto, descrizione, saldo_finale
            FROM ricavi_contabili WHERE anno=%s AND mese=%s AND livello=0 ORDER BY conto
        """, (row['anno'], m)).fetchall()
        ricavi_profis_per_mese[m] = {}
        for r in righe:
            ricavi_profis_per_mese[m][r['conto']] = float(r['saldo_finale'])
            if r['conto'] not in conti_ricavi_tutti:
                conti_ricavi_tutti[r['conto']] = r['descrizione']

        tot = conn.execute("""
            SELECT COALESCE(SUM(saldo_non_rettificato),0) as nr,
                   COALESCE(SUM(rettifiche),0) as rt,
                   COALESCE(SUM(saldo_finale),0) as sf
            FROM ricavi_contabili WHERE anno=%s AND mese=%s AND livello=0
        """, (str(anno), m)).fetchone()
        ricavi_profis_per_mese[m]['_totali'] = {
            'non_rett': float(tot['nr']),
            'rettifiche': float(tot['rt']),
            'saldo': float(tot['sf']),
        }

    conti_ricavi_ordinati = sorted(k for k in conti_ricavi_tutti.keys())

    # Costruisci riga per ogni mese
    mesi_pl = []
    for m in range(1, 13):
        ric = ricavi_profis_per_mese.get(m, {}).get('_totali', {'non_rett': 0.0, 'rettifiche': 0.0, 'saldo': 0.0})
        ricavi_m = {k: v for k, v in ricavi_profis_per_mese.get(m, {}).items() if k != '_totali'}
        cos_tot = costi_per_mese.get(m, {}).get('_totali', {'non_rett': 0.0, 'rettifiche': 0.0, 'saldo': 0.0})
        costi_m = {k: v for k, v in costi_per_mese.get(m, {}).items() if k != '_totali'}
        risultato = round(ric['saldo'] - cos_tot['saldo'], 2)
        margine   = round(risultato / ric['saldo'] * 100, 1) if ric['saldo'] > 0 else 0.0
        mesi_pl.append({
            'mese': m,
            'nome': MESI_IT[m],
            'nome_breve': MESI_IT[m][:3],
            'ha_dati': m in mesi_con_dati,
            'ha_ricavi': m in mesi_con_ricavi_profis,
            'ricavi_non_rett': ric['non_rett'],
            'ricavi_rettifiche': ric['rettifiche'],
            'ricavi_saldo': ric['saldo'],
            'costi_non_rett': cos_tot['non_rett'],
            'costi_rettifiche': cos_tot['rettifiche'],
            'costi_saldo': cos_tot['saldo'],
            'ricavi': ricavi_m,
            'costi': costi_m,
            'risultato': risultato,
            'margine': margine,
        })

    tot_anno = {
        'ricavi_non_rett': sum(r['ricavi_non_rett'] for r in mesi_pl),
        'ricavi_rettifiche': sum(r['ricavi_rettifiche'] for r in mesi_pl),
        'ricavi_saldo':    sum(r['ricavi_saldo']  for r in mesi_pl),
        'costi_non_rett':  sum(r['costi_non_rett'] for r in mesi_pl),
        'costi_rettifiche': sum(r['costi_rettifiche'] for r in mesi_pl),
        'costi_saldo':     sum(r['costi_saldo']   for r in mesi_pl),
        'risultato':       sum(r['risultato']     for r in mesi_pl),
        'conti': {c: sum(r['costi'].get(c, 0.0) for r in mesi_pl) for c in conti_ordinati},
        'conti_ricavi': {c: sum(r['ricavi'].get(c, 0.0) for r in mesi_pl) for c in conti_ricavi_ordinati},
    }
    tot_anno['margine'] = round(
        tot_anno['risultato'] / tot_anno['ricavi_saldo'] * 100, 1
    ) if tot_anno['ricavi_saldo'] > 0 else 0.0

    # Anni disponibili = unione anni contabili + ricavi contabili + anni movimenti
    anni_cont = {str(r[0]) for r in conn.execute(
        "SELECT DISTINCT anno FROM costi_contabili ORDER BY anno DESC"
    ).fetchall()}
    anni_ric = {str(r[0]) for r in conn.execute(
        "SELECT DISTINCT anno FROM ricavi_contabili ORDER BY anno DESC"
    ).fetchall()}
    anni_mov  = {str(r[0]) for r in conn.execute(
        "SELECT DISTINCT LEFT(data, 4) FROM movimenti ORDER BY 1 DESC"
    ).fetchall()}
    anni = sorted(anni_cont | anni_ric | anni_mov, reverse=True)
    if str(anno) not in anni:
        anni.insert(0, str(anno))

    # Dati raw per la tabella in fondo (selettore mese + tipo)
    mesi_disp_costi = {r['mese'] for r in conn.execute(
        "SELECT DISTINCT mese FROM costi_contabili WHERE anno=%s ORDER BY mese", (str(anno),)
    ).fetchall()}
    mesi_disp_ricavi = {r['mese'] for r in conn.execute(
        "SELECT DISTINCT mese FROM ricavi_contabili WHERE anno=%s ORDER BY mese", (str(anno),)
    ).fetchall()}
    mesi_disponibili = sorted(mesi_disp_costi | mesi_disp_ricavi)

    mese_sel = request.args.get('mese', type=int)
    tipo_sel = request.args.get('tipo', 'costi')
    if tipo_sel not in ('costi', 'ricavi'):
        tipo_sel = 'costi'
    if mese_sel is None and mesi_disponibili:
        mese_sel = mesi_disponibili[-1]

    voci_mese = []
    totale_mese = 0.0
    if mese_sel:
        tabella = 'ricavi_contabili' if tipo_sel == 'ricavi' else 'costi_contabili'
        voci_mese = conn.execute(
            f"SELECT * FROM {tabella} WHERE anno=%s AND mese=%s ORDER BY conto",
            (str(anno), mese_sel)
        ).fetchall()
        totale_mese = sum(v['saldo_finale'] for v in voci_mese if v['livello'] == 0)

    conn.close()
    return render_template("pl.html",
        anno=anno,
        anni=anni,
        mesi_pl=mesi_pl,
        tot_anno=tot_anno,
        conti_ordinati=conti_ordinati,
        conti_desc=conti_tutti,
        conti_ricavi_ordinati=conti_ricavi_ordinati,
        conti_ricavi_desc=conti_ricavi_tutti,
        mesi_it=MESI_IT,
        mesi_disponibili=mesi_disponibili,
        mesi_disp_costi=mesi_disp_costi,
        mesi_disp_ricavi=mesi_disp_ricavi,
        mesi_con_ricavi_profis=mesi_con_ricavi_profis,
        mese_sel=mese_sel,
        tipo_sel=tipo_sel,
        voci_mese=voci_mese,
        totale_mese=totale_mese,
    )


@app.route("/pl/export-csv/<int:anno>/<int:mese>")
@login_required
def pl_export_csv(anno, mese):
    conn = get_connection()
    voci = conn.execute("""
        SELECT conto, descrizione, saldo_non_rettificato, rettifiche, saldo_finale
        FROM costi_contabili WHERE anno=%s AND mese=%s ORDER BY conto
    """, (anno, mese)).fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Conto', 'Descrizione', 'Saldo non rettificato', 'Rettifiche', 'Saldo finale'])
    for v in voci:
        writer.writerow([v['conto'], v['descrizione'],
                         v['saldo_non_rettificato'], v['rettifiche'], v['saldo_finale']])
    nome_mese = MESI_IT.get(mese, str(mese)).lower()
    filename = f"profis_{anno}_{nome_mese}.csv"
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename={filename}'})


@app.route("/cfo")
@login_required
def cfo():
    conn = get_connection()
    anni_cfo = [r[0] for r in conn.execute(
        "SELECT DISTINCT CAST(LEFT(data, 4) AS INTEGER) FROM movimenti WHERE data IS NOT NULL ORDER BY 1 DESC"
    ).fetchall()]
    cy = datetime.date.today().year
    if cy not in anni_cfo:
        anni_cfo.insert(0, cy)
    anni_cfo = sorted(set(anni_cfo), reverse=True)

    anno_sel = request.args.get('anno', '')
    anno_ref = int(anno_sel) if anno_sel and anno_sel.isdigit() else cy

    data = _build_cfo_data(conn, anno_ref=anno_ref)
    conn.close()
    return render_template("cfo.html", **data, anni_cfo=anni_cfo, anno_cfo=anno_ref)


@app.route("/cfo/chat", methods=["POST"])
@login_required
def cfo_chat():
    try:
        from openai import OpenAI
    except ImportError:
        return jsonify({'error': 'Libreria OpenAI non installata. Esegui: pip install openai'}), 500

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return jsonify({'error': 'OPENAI_API_KEY non configurata. Aggiungila nel file .env nella cartella del progetto.'}), 500

    body = request.json or {}
    message = body.get('message', '').strip()
    history = body.get('history', [])  # lista di {role, content} dal client
    anno_chat = body.get('anno_ref')
    if not message:
        return jsonify({'error': 'Messaggio vuoto'}), 400

    conn = get_connection()
    d = _build_cfo_data(conn, anno_ref=int(anno_chat) if anno_chat else None)
    conn.close()

    run_label = f"{d['runway_mesi']:.0f} mesi" if d['runway_mesi'] < 99 else "oltre 12 mesi"
    cashflow_lines = "\n".join(
        f"  {p['mese']}: entrate €{p['entrate']:,.0f}, uscite €{p['uscite']:,.0f}, netto €{p['netto_base']:,.0f}, saldo cumulativo €{p['saldo_cumulativo']:,.0f}"
        for p in d['cashflow_proj']
    )
    alert_lines = "\n".join(f"  [{a['tipo'].upper()}] {a['titolo']} — {a['dettaglio']}" for a in d['alerts'])
    servizi_txt = ", ".join(f"{s['servizio']} (ticket medio €{s['ticket_medio']:,.0f})" for s in d.get('ticket_per_servizio', []))

    system_context = f"""Sei il CFO virtuale dell'azienda Thrive X. Rispondi SEMPRE in italiano, in modo diretto, pratico e professionale. Usa i dati reali dell'azienda nelle tue risposte. Sii specifico: cita numeri, mesi, percentuali. Non essere vago. Ricordi tutta la conversazione precedente con l'utente.

SITUAZIONE FINANZIARIA AGGIORNATA:
- Saldo conto attuale: €{d['saldo_attuale']:,.2f}
- Entrate ultimi 3 mesi: €{d['entrate_3m']:,.2f}
- Uscite ultimi 3 mesi: €{d['uscite_3m']:,.2f}
- Margine operativo (3m): {d['margine_pct']:.1f}%
- Trend ricavi (vs 3m prec.): {d['revenue_trend_pct']:+.1f}%

RICAVI RICORRENTI:
- MRR: €{d['mrr']:,.2f} (lordo), €{d['mrr_netto']:,.2f} (netto quota nostra)
- ARR: €{d['arr']:,.2f}
- Contratti attivi: {d['num_contratti_attivi']}
- Clienti attivi: {d['num_clienti_attivi']}

COSTI E LIQUIDITÀ:
- Burn rate mensile: €{d['burn_rate']:,.2f}
- Cash runway: {run_label}

PIPELINE COMMERCIALE:
- Valore pipeline aperta: €{d['pipeline_aperta']:,.2f}
- Contratti firmati: €{d['pipeline_firmata']:,.2f}
- Tasso conversione: {d['conversion_rate']:.1f}%

BUSINESS HEALTH SCORE: {d['health_score']}/100

REVENUE ENGINE:
- Churn rate annuo: {d['churn_rate_pct']:.1f}% | Renewal rate: {d['renewal_rate_pct']:.1f}%
- Durata media contratti: {d['durata_media_mesi']:.1f} mesi
- % fatturato ricorrente: {d['pct_ricorrente']:.0f}%
- Servizi attivi: {servizi_txt if servizi_txt else 'n/d'}

INDICATORI DI EFFICIENZA:
- DSO (Days Sales Outstanding): {d['dso_giorni']:.0f} giorni
- Ritardo medio pagamento: {d['ritardo_medio_gg']:.0f} giorni oltre scadenza
- Cash Conversion Ratio {d['today'][:4]}: {d['cash_conversion_ratio']:.1f}% (incassato/emesso)
- LTV stimato per cliente: €{d['ltv_stimato']:,.0f}
- MRR netto per cliente: €{d['mrr_per_cliente']:,.0f}

PROIEZIONE CASH FLOW (prossimi 12 mesi):
{cashflow_lines}

ALERT ATTIVI:
{alert_lines}"""

    # Costruisce la lista messaggi: system + tutta la history + nuovo messaggio
    safe_history = [
        {"role": m["role"], "content": m["content"]}
        for m in history
        if m.get("role") in ("user", "assistant") and m.get("content", "").strip()
    ][-20:]  # max ultimi 20 turni per non sforare il context

    messages = (
        [{"role": "system", "content": system_context}]
        + safe_history
        + [{"role": "user", "content": message}]
    )

    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=900,
        temperature=0.3,
    )
    return jsonify({'response': resp.choices[0].message.content})


# ─── PROIEZIONI ────────────────────────────────────────────────────────────────

@app.route("/proiezioni")
@login_required
def proiezioni():
    anno = datetime.date.today().year
    conn = get_connection()

    uscite = conn.execute(
        "SELECT * FROM proiezioni_uscite WHERE anno=%s ORDER BY mese_inizio, nome",
        (anno,)
    ).fetchall()

    # Ricavi lordi per mese + quota fornitore (costo)
    ricavi_rows = conn.execute("""
        SELECT
            CAST(SUBSTRING(rc.data_scadenza FROM 6 FOR 2) AS INTEGER) as mese,
            rc.importo,
            COALESCE(ct.percentuale_partner, 0) as percentuale_partner
        FROM rate_contratto rc
        JOIN contratti ct ON rc.contratto_id = ct.id
        WHERE LEFT(rc.data_scadenza, 4) = %s
    """, (str(anno),)).fetchall()

    ricavi_per_mese   = [0.0] * 13
    fornitori_per_mese = [0.0] * 13
    for r in ricavi_rows:
        m = r['mese']
        importo = float(r['importo'])
        pct = float(r['percentuale_partner'])
        ricavi_per_mese[m] += importo
        fornitori_per_mese[m] += round(importo * pct / 100, 2)

    SKIP_MESI = {'mensile': 1, 'bimestrale': 2, 'trimestrale': 3, 'semestrale': 6, 'annuale': 12}

    # Uscite per mese da proiezioni_uscite (+ quota fornitore aggiunta sotto)
    uscite_per_mese = [0.0] * 13
    for u in uscite:
        mese_inizio = u['mese_inizio']
        durata = u['durata_mesi']
        importo = float(u['importo_mensile'])
        skip = SKIP_MESI.get(u['ricorrenza'] or 'mensile', 1)
        mese_fine = 12 if durata is None else min(mese_inizio + durata - 1, 12)
        m = mese_inizio
        while m <= mese_fine:
            uscite_per_mese[m] += importo
            m += skip

    oggi = datetime.date.today()
    # Costi totali = uscite proiettate + quota fornitore da ricavi
    costi_totali = [round(uscite_per_mese[m] + fornitori_per_mese[m], 2) for m in range(13)]

    chart_labels = json.dumps([MESI_IT[m] for m in range(1, 13)])
    chart_ricavi = json.dumps([round(ricavi_per_mese[m], 2) for m in range(1, 13)])
    chart_uscite = json.dumps([costi_totali[m] for m in range(1, 13)])
    chart_delta  = json.dumps([round(ricavi_per_mese[m] - costi_totali[m], 2) for m in range(1, 13)])

    conn.close()
    return render_template("proiezioni.html",
        anno=anno,
        uscite=uscite,
        mesi_it=MESI_IT,
        oggi_mese=oggi.month,
        chart_labels=chart_labels,
        chart_ricavi=chart_ricavi,
        chart_uscite=chart_uscite,
        chart_delta=chart_delta,
    )


@app.route("/proiezioni/importa", methods=["POST"])
@login_required
def proiezioni_importa():
    anno = datetime.date.today().year
    conn = get_connection()

    already = {row[0] for row in conn.execute(
        "SELECT scadenza_id FROM proiezioni_uscite WHERE anno=%s AND scadenza_id IS NOT NULL",
        (anno,)
    ).fetchall()}

    rows = conn.execute("""
        SELECT sc.id, sc.nome, sc.uscita_cassa_rata, sc.ricorrenza,
               COUNT(*) as num_rate_anno
        FROM scadenze_costi sc
        JOIN rate_scadenza_costo rsc ON rsc.scadenza_costo_id = sc.id
        WHERE LEFT(rsc.data_scadenza, 4) = %s
        GROUP BY sc.id, sc.nome, sc.uscita_cassa_rata, sc.ricorrenza
    """, (str(anno),)).fetchall()

    for r in rows:
        if r['id'] in already:
            continue

        if r['ricorrenza'] == 'mensile':
            primo_mese = conn.execute("""
                SELECT MIN(CAST(SUBSTRING(data_scadenza FROM 6 FOR 2) AS INTEGER))
                FROM rate_scadenza_costo
                WHERE scadenza_costo_id = %s AND LEFT(data_scadenza, 4) = %s
            """, (r['id'], str(anno))).fetchone()[0] or 1
            conn.execute("""
                INSERT INTO proiezioni_uscite
                    (nome, importo_mensile, tipo, mese_inizio, durata_mesi, anno, scadenza_id)
                VALUES (%s, %s, 'fisso', %s, %s, %s, %s)
            """, (r['nome'], r['uscita_cassa_rata'], primo_mese, r['num_rate_anno'], anno, r['id']))
        else:
            months = conn.execute("""
                SELECT CAST(SUBSTRING(data_scadenza FROM 6 FOR 2) AS INTEGER) as mese
                FROM rate_scadenza_costo
                WHERE scadenza_costo_id = %s AND LEFT(data_scadenza, 4) = %s
                ORDER BY data_scadenza
            """, (r['id'], str(anno))).fetchall()
            for month_row in months:
                conn.execute("""
                    INSERT INTO proiezioni_uscite
                        (nome, importo_mensile, tipo, mese_inizio, durata_mesi, anno, scadenza_id)
                    VALUES (%s, %s, 'fisso', %s, 1, %s, %s)
                """, (r['nome'], r['uscita_cassa_rata'], month_row['mese'], anno, r['id']))

    conn.commit()
    conn.close()
    return redirect(url_for('proiezioni'))


@app.route("/proiezioni/uscite/nuovo", methods=["POST"])
@login_required
def nuova_proiezione_uscita():
    anno = datetime.date.today().year
    nome = request.form.get('nome', '').strip()
    importo = float(request.form.get('importo_mensile', 0) or 0)
    tipo = request.form.get('tipo', 'fisso')
    ricorrenza = request.form.get('ricorrenza', 'mensile')
    mese_inizio = int(request.form.get('mese_inizio', 1))
    durata_raw = request.form.get('durata_mesi', '').strip()
    durata_mesi = int(durata_raw) if durata_raw else None
    note = request.form.get('note', '').strip() or None

    conn = get_connection()
    conn.execute("""
        INSERT INTO proiezioni_uscite
            (nome, importo_mensile, tipo, ricorrenza, mese_inizio, durata_mesi, anno, note)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (nome, importo, tipo, ricorrenza, mese_inizio, durata_mesi, anno, note))
    conn.commit()
    conn.close()
    return redirect(url_for('proiezioni'))


@app.route("/proiezioni/uscite/<int:uid>/modifica", methods=["POST"])
@login_required
def modifica_proiezione_uscita(uid):
    nome = request.form.get('nome', '').strip()
    importo = float(request.form.get('importo_mensile', 0) or 0)
    tipo = request.form.get('tipo', 'fisso')
    ricorrenza = request.form.get('ricorrenza', 'mensile')
    mese_inizio = int(request.form.get('mese_inizio', 1))
    durata_raw = request.form.get('durata_mesi', '').strip()
    durata_mesi = int(durata_raw) if durata_raw else None
    note = request.form.get('note', '').strip() or None

    conn = get_connection()
    conn.execute("""
        UPDATE proiezioni_uscite
        SET nome=%s, importo_mensile=%s, tipo=%s, ricorrenza=%s, mese_inizio=%s, durata_mesi=%s, note=%s
        WHERE id=%s
    """, (nome, importo, tipo, ricorrenza, mese_inizio, durata_mesi, note, uid))
    conn.commit()
    conn.close()
    return redirect(url_for('proiezioni'))


@app.route("/proiezioni/uscite/<int:uid>/elimina", methods=["POST"])
@login_required
def elimina_proiezione_uscita(uid):
    conn = get_connection()
    conn.execute("DELETE FROM proiezioni_uscite WHERE id=%s", (uid,))
    conn.commit()
    conn.close()
    return redirect(url_for('proiezioni'))


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5050)
