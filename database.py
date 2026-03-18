import os
import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
except ImportError:
    pass


class PgCursor:
    """Wrapper around psycopg2 cursor that returns self from execute() to allow chaining."""

    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, params=None):
        self._cur.execute(sql, params)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def executemany(self, sql, seq):
        self._cur.executemany(sql, seq)
        return self

    def __iter__(self):
        return iter(self._cur)

    def __getattr__(self, name):
        return getattr(self._cur, name)


class PgConn:
    """Thin wrapper around psycopg2 connection that mimics sqlite3's conn.execute() API."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = PgCursor(self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor))
        cur.execute(sql, params)
        return cur

    def cursor(self):
        return PgCursor(self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor))

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._conn.rollback()
        self.close()


def get_connection():
    url = os.environ.get('DATABASE_URL')
    if not url:
        raise RuntimeError('DATABASE_URL environment variable not set')
    conn = psycopg2.connect(url)
    return PgConn(conn)


def _get_columns(c, table):
    c.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
    """, (table,))
    return {row[0] for row in c.fetchall()}


def _get_tables(c):
    c.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    return {row[0] for row in c.fetchall()}


def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS partners (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL DEFAULT 'operativo',
            email TEXT,
            telefono TEXT,
            percentuale_default REAL DEFAULT 60.0,
            note TEXT,
            creato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS clienti (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            email TEXT,
            telefono TEXT,
            note TEXT,
            sorgente TEXT DEFAULT 'diretto',
            partner_id INTEGER REFERENCES partners(id) ON DELETE SET NULL,
            creato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS opportunita (
            id SERIAL PRIMARY KEY,
            nome_azienda TEXT NOT NULL,
            contatto TEXT,
            email TEXT,
            telefono TEXT,
            servizio TEXT,
            valore_stimato REAL DEFAULT 0,
            sorgente TEXT DEFAULT 'evento',
            partner_id INTEGER REFERENCES partners(id) ON DELETE SET NULL,
            stato TEXT NOT NULL DEFAULT 'lead',
            note TEXT,
            data_creazione TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD')
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS contratti (
            id SERIAL PRIMARY KEY,
            cliente_id INTEGER NOT NULL REFERENCES clienti(id) ON DELETE CASCADE,
            titolo TEXT NOT NULL,
            servizio TEXT,
            importo_totale REAL NOT NULL,
            percentuale_partner REAL DEFAULT 60.0,
            partner_id INTEGER REFERENCES partners(id) ON DELETE SET NULL,
            tipo_pagamento TEXT DEFAULT 'split_50',
            stato TEXT DEFAULT 'attivo',
            data_inizio TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD'),
            note TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS rate_contratto (
            id SERIAL PRIMARY KEY,
            contratto_id INTEGER NOT NULL REFERENCES contratti(id) ON DELETE CASCADE,
            numero_rata INTEGER NOT NULL,
            importo REAL NOT NULL,
            data_scadenza TEXT,
            pagato INTEGER DEFAULT 0,
            data_pagamento TEXT,
            note TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS movimenti (
            id SERIAL PRIMARY KEY,
            tipo TEXT NOT NULL CHECK(tipo IN ('entrata', 'uscita')),
            descrizione TEXT NOT NULL,
            importo REAL NOT NULL,
            cliente_id INTEGER REFERENCES clienti(id) ON DELETE SET NULL,
            contratto_id INTEGER,
            rata_id INTEGER,
            data TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD'),
            categoria TEXT DEFAULT 'altro'
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS budget (
            id SERIAL PRIMARY KEY,
            anno INTEGER NOT NULL,
            mese INTEGER NOT NULL,
            categoria TEXT NOT NULL,
            tipo TEXT NOT NULL CHECK(tipo IN ('entrata', 'uscita')),
            importo REAL NOT NULL DEFAULT 0,
            creato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS'),
            UNIQUE(anno, mese, categoria, tipo)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS impostazioni (
            chiave TEXT PRIMARY KEY,
            valore TEXT NOT NULL
        )
    """)

    c.execute("""
        INSERT INTO impostazioni (chiave, valore) VALUES ('saldo_iniziale_conto', '0')
        ON CONFLICT DO NOTHING
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS scadenze_costi (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            categoria TEXT NOT NULL DEFAULT 'altro',
            importo_rata REAL NOT NULL,
            uscita_cassa_rata REAL NOT NULL DEFAULT 0,
            ricorrenza TEXT NOT NULL DEFAULT 'mensile',
            data_prima_scadenza TEXT NOT NULL,
            num_rate INTEGER NOT NULL,
            note TEXT,
            creato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS rate_scadenza_costo (
            id SERIAL PRIMARY KEY,
            scadenza_costo_id INTEGER NOT NULL REFERENCES scadenze_costi(id) ON DELETE CASCADE,
            numero_rata INTEGER NOT NULL,
            importo REAL NOT NULL,
            uscita_cassa REAL NOT NULL DEFAULT 0,
            data_scadenza TEXT NOT NULL,
            pagato INTEGER DEFAULT 0,
            data_pagamento TEXT,
            note TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS costi_contabili (
            id SERIAL PRIMARY KEY,
            anno INTEGER NOT NULL,
            mese INTEGER NOT NULL,
            conto TEXT NOT NULL,
            livello INTEGER NOT NULL DEFAULT 0,
            flag TEXT,
            descrizione TEXT NOT NULL,
            note TEXT,
            saldo_non_rettificato REAL,
            rettifiche REAL,
            saldo_finale REAL NOT NULL DEFAULT 0,
            importato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS'),
            UNIQUE(anno, mese, conto)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS ricavi_contabili (
            id SERIAL PRIMARY KEY,
            anno INTEGER NOT NULL,
            mese INTEGER NOT NULL,
            conto TEXT NOT NULL,
            livello INTEGER NOT NULL DEFAULT 0,
            flag TEXT,
            descrizione TEXT NOT NULL,
            note TEXT,
            saldo_non_rettificato REAL,
            rettifiche REAL,
            saldo_finale REAL NOT NULL DEFAULT 0,
            importato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS'),
            UNIQUE(anno, mese, conto)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS riconciliazione_regole (
            id SERIAL PRIMARY KEY,
            pattern TEXT NOT NULL,
            importo_esatto REAL,
            nome_costo TEXT NOT NULL,
            categoria TEXT NOT NULL DEFAULT 'altro',
            raggruppa_per_mese INTEGER NOT NULL DEFAULT 0,
            creata_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS proiezioni_uscite (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            importo_mensile REAL NOT NULL,
            tipo TEXT NOT NULL DEFAULT 'fisso',
            ricorrenza TEXT NOT NULL DEFAULT 'mensile',
            mese_inizio INTEGER NOT NULL DEFAULT 1,
            durata_mesi INTEGER,
            anno INTEGER NOT NULL DEFAULT 2026,
            note TEXT,
            scadenza_id INTEGER,
            creato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    conn.commit()
    _migrate(conn)
    conn.close()


def _migrate(conn):
    """Add new columns to existing tables without breaking existing data."""
    c = conn.cursor()

    # clienti: add sorgente, partner_id
    existing = _get_columns(c, 'clienti')
    if 'sorgente' not in existing:
        c.execute("ALTER TABLE clienti ADD COLUMN sorgente TEXT DEFAULT 'diretto'")
    if 'partner_id' not in existing:
        c.execute("ALTER TABLE clienti ADD COLUMN partner_id INTEGER")

    # movimenti: add contratto_id, rata_id, categoria, codice_banca
    existing = _get_columns(c, 'movimenti')
    if 'contratto_id' not in existing:
        c.execute("ALTER TABLE movimenti ADD COLUMN contratto_id INTEGER")
    if 'rata_id' not in existing:
        c.execute("ALTER TABLE movimenti ADD COLUMN rata_id INTEGER")
    if 'categoria' not in existing:
        c.execute("ALTER TABLE movimenti ADD COLUMN categoria TEXT DEFAULT 'altro'")
    if 'codice_banca' not in existing:
        c.execute("ALTER TABLE movimenti ADD COLUMN codice_banca TEXT")
    # Indice univoco su codice_banca — prima rimuovi eventuali duplicati, poi crea l'indice
    c.execute("""
        DELETE FROM movimenti WHERE id NOT IN (
            SELECT MIN(id) FROM movimenti
            WHERE codice_banca IS NOT NULL
            GROUP BY codice_banca
        ) AND codice_banca IS NOT NULL
    """)
    try:
        c.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_movimenti_codice_banca_unique
            ON movimenti(codice_banca)
            WHERE codice_banca IS NOT NULL
        """)
    except Exception:
        pass  # Indice già esistente

    # contratti: add data_fine, data_firma
    existing = _get_columns(c, 'contratti')
    if 'data_fine' not in existing:
        c.execute("ALTER TABLE contratti ADD COLUMN data_fine TEXT")
    if 'data_firma' not in existing:
        c.execute("ALTER TABLE contratti ADD COLUMN data_firma TEXT")

    # rate_contratto: add fatturato, data_fatturazione, movimento_id
    existing = _get_columns(c, 'rate_contratto')
    if 'fatturato' not in existing:
        c.execute("ALTER TABLE rate_contratto ADD COLUMN fatturato INTEGER DEFAULT 0")
    if 'data_fatturazione' not in existing:
        c.execute("ALTER TABLE rate_contratto ADD COLUMN data_fatturazione TEXT")
    if 'movimento_id' not in existing:
        c.execute("ALTER TABLE rate_contratto ADD COLUMN movimento_id INTEGER")

    # scadenze_costi: add uscita_cassa_rata
    existing = _get_columns(c, 'scadenze_costi')
    if 'uscita_cassa_rata' not in existing:
        c.execute("ALTER TABLE scadenze_costi ADD COLUMN uscita_cassa_rata REAL NOT NULL DEFAULT 0")

    # rate_scadenza_costo: add uscita_cassa, csv_hash, csv_descrizione
    existing = _get_columns(c, 'rate_scadenza_costo')
    if 'uscita_cassa' not in existing:
        c.execute("ALTER TABLE rate_scadenza_costo ADD COLUMN uscita_cassa REAL NOT NULL DEFAULT 0")
    if 'csv_hash' not in existing:
        c.execute("ALTER TABLE rate_scadenza_costo ADD COLUMN csv_hash TEXT")
    if 'csv_descrizione' not in existing:
        c.execute("ALTER TABLE rate_scadenza_costo ADD COLUMN csv_descrizione TEXT")

    # Regole preset riconciliazione (solo se vuota)
    c.execute("SELECT COUNT(*) FROM riconciliazione_regole")
    count = c.fetchone()[0]
    if count == 0:
        c.execute("""INSERT INTO riconciliazione_regole (pattern, nome_costo, categoria, raggruppa_per_mese)
                     VALUES ('commissione bonifico', 'Commissioni bonifici', 'banca', 1)""")
        c.execute("""INSERT INTO riconciliazione_regole (pattern, nome_costo, categoria, raggruppa_per_mese)
                     VALUES ('spese bonifico', 'Commissioni bonifici', 'banca', 1)""")
        c.execute("""INSERT INTO riconciliazione_regole (pattern, nome_costo, categoria, raggruppa_per_mese)
                     VALUES ('canone', 'Canone bancario', 'banca', 0)""")

    # riconciliazione_regole: add scadenza_id
    existing = _get_columns(c, 'riconciliazione_regole')
    if 'scadenza_id' not in existing:
        c.execute("ALTER TABLE riconciliazione_regole ADD COLUMN scadenza_id INTEGER")

    # Rimuovi regola generica pagoPA
    c.execute("DELETE FROM riconciliazione_regole WHERE pattern='pagamento pagopa' AND scadenza_id IS NULL")

    # proiezioni_uscite: add ricorrenza column
    existing = _get_columns(c, 'proiezioni_uscite')
    if 'ricorrenza' not in existing:
        c.execute("ALTER TABLE proiezioni_uscite ADD COLUMN ricorrenza TEXT NOT NULL DEFAULT 'mensile'")

    # finanziamenti bancari
    c.execute("""
        CREATE TABLE IF NOT EXISTS finanziamenti (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            banca TEXT,
            importo_originale REAL NOT NULL DEFAULT 0,
            data_inizio TEXT,
            data_fine TEXT,
            note TEXT,
            creato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS rate_finanziamento (
            id SERIAL PRIMARY KEY,
            finanziamento_id INTEGER NOT NULL REFERENCES finanziamenti(id) ON DELETE CASCADE,
            data_scadenza TEXT NOT NULL,
            rata_totale REAL NOT NULL DEFAULT 0,
            quota_capitale REAL NOT NULL DEFAULT 0,
            quota_interessi REAL NOT NULL DEFAULT 0,
            pagato INTEGER DEFAULT 0,
            data_pagamento TEXT
        )
    """)

    # piani di rientro fiscali
    c.execute("""
        CREATE TABLE IF NOT EXISTS piani_rientro (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL DEFAULT 'contributi',
            ente TEXT,
            anno_riferimento INTEGER,
            note TEXT,
            creato_il TEXT DEFAULT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS rate_piano_rientro (
            id SERIAL PRIMARY KEY,
            piano_rientro_id INTEGER NOT NULL REFERENCES piani_rientro(id) ON DELETE CASCADE,
            data_scadenza TEXT NOT NULL,
            importo REAL NOT NULL DEFAULT 0,
            quota_interessi REAL NOT NULL DEFAULT 0,
            pagato INTEGER DEFAULT 0,
            data_pagamento TEXT
        )
    """)
    # Migrazione: aggiungi quota_interessi se mancante
    existing = _get_columns(c, 'rate_piano_rientro')
    if 'quota_interessi' not in existing:
        c.execute("ALTER TABLE rate_piano_rientro ADD COLUMN quota_interessi REAL NOT NULL DEFAULT 0")

    # Aggiungi pattern aggiuntivi se mancanti
    preset_aggiuntivi = [
        ('comm.bon', 'Commissioni bonifici', 'banca', 1),
        ('giroconto mutuo', 'Giroconto mutuo', 'banca', 0),
        ('fondo pensione', 'Fondo pensione', 'altro', 0),
        ('iubenda', 'iubenda', 'software', 0),
    ]
    for pattern, nome, cat, raggruppa in preset_aggiuntivi:
        c.execute("SELECT id FROM riconciliazione_regole WHERE pattern=%s", (pattern,))
        if not c.fetchone():
            c.execute("""INSERT INTO riconciliazione_regole (pattern, nome_costo, categoria, raggruppa_per_mese)
                         VALUES (%s, %s, %s, %s)""", (pattern, nome, cat, raggruppa))

    # Seed: Mutuo Chirografario Banca Sondrio — inserisce solo se non già presente
    c.execute("SELECT id FROM finanziamenti WHERE nome='Mutuo Chirografario - Banca Sondrio'")
    if not c.fetchone():
        c.execute("""
            INSERT INTO finanziamenti (nome, banca, importo_originale, data_inizio, data_fine, note)
            VALUES ('Mutuo Chirografario - Banca Sondrio', 'Banca Sondrio', 150000.00,
                    '2022-12-01', '2028-11-01',
                    'Finanziamento 10 1429635 - CHCOP FCGV MCC Legge 662')
            RETURNING id
        """)
        fin_id = c.fetchone()[0]
        # (data_scadenza, rata_totale, quota_capitale, quota_interessi, pagato)
        # pagato=1 per rate con scadenza <= 2026-03-01
        rate_sondrio = [
            ('2022-12-01', 809.46,   0.00,    806.46, 1),
            ('2023-01-01', 616.15,   0.00,    613.15, 1),
            ('2023-02-01', 668.34,   0.00,    665.34, 1),
            ('2023-03-01', 632.30,   0.00,    629.30, 1),
            ('2023-04-01', 735.63,   0.00,    732.63, 1),
            ('2023-05-01', 774.37,   0.00,    771.37, 1),
            ('2023-06-01', 815.07,   0.00,    812.07, 1),
            ('2023-07-01', 811.00,   0.00,    808.00, 1),
            ('2023-08-01', 863.90,   0.00,    860.90, 1),
            ('2023-09-01', 885.85,   0.00,    882.85, 1),
            ('2023-10-01', 866.00,   0.00,    863.00, 1),
            ('2023-11-01', 919.44,   0.00,    916.44, 1),
            ('2023-12-01', 3161.23,  2265.35, 892.88, 1),
            ('2024-01-01', 3179.55,  2272.81, 903.74, 1),
            ('2024-02-01', 3175.38,  2280.29, 892.09, 1),
            ('2024-03-01', 3112.25,  2287.79, 821.46, 1),
            ('2024-04-01', 3162.40,  2295.32, 864.08, 1),
            ('2024-05-01', 3128.46,  2302.88, 822.58, 1),
            ('2024-06-01', 3149.34,  2310.46, 835.88, 1),
            ('2024-07-01', 3116.27,  2318.07, 795.20, 1),
            ('2024-08-01', 3136.19,  2325.70, 807.49, 1),
            ('2024-09-01', 3129.58,  2333.35, 793.23, 1),
            ('2024-10-01', 3097.82,  2341.03, 753.79, 1),
            ('2024-11-01', 3116.30,  2348.74, 764.56, 1),
            ('2024-12-01', 3085.43,  2356.47, 725.96, 1),
            ('2025-01-01', 3102.94,  2364.23, 735.71, 1),
            ('2025-02-01', 3096.22,  2372.01, 721.21, 1),
            ('2025-03-01', 3021.09,  2379.82, 638.27, 1),
            ('2025-04-01', 3082.71,  2387.65, 692.06, 1),
            ('2025-05-01', 3054.08,  2395.51, 655.57, 1),
            ('2025-06-01', 3069.12,  2403.39, 662.73, 1),
            ('2025-07-01', 3041.40,  2411.31, 627.09, 1),
            ('2025-08-01', 3055.44,  2419.24, 633.20, 1),
            ('2025-09-01', 3048.57,  2427.21, 618.36, 1),
            ('2025-10-01', 3022.21,  2435.20, 584.01, 1),
            ('2025-11-01', 3034.75,  2443.21, 588.54, 1),
            ('2025-12-01', 3009.31,  2451.25, 555.06, 1),
            ('2026-01-01', 3020.85,  2459.32, 558.53, 1),
            ('2026-02-01', 3013.87,  2467.42, 543.45, 1),
            ('2026-03-01', 2955.73,  2475.54, 477.19, 1),
            ('2026-04-01', 2999.82,  2483.69, 513.13, 0),
            ('2026-05-01', 2976.70,  2491.86, 481.84, 0),
            ('2026-06-01', 2985.69,  2500.07, 482.62, 0),
            ('2026-07-01', 2963.51,  2508.30, 452.21, 0),
            ('2026-08-01', 2971.45,  2516.55, 451.90, 0),
            ('2026-09-01', 2964.31,  2524.84, 436.47, 0),
            ('2026-10-01', 2943.55,  2533.15, 407.40, 0),
            ('2026-11-01', 2949.93,  2541.48, 405.45, 0),
            ('2026-12-01', 2930.13,  2549.85, 377.28, 0),
            ('2027-01-01', 2935.46,  2558.24, 374.22, 0),
            ('2027-02-01', 2928.19,  2566.66, 358.53, 0),
            ('2027-03-01', 2887.73,  2575.11, 309.62, 0),
            ('2027-04-01', 2913.59,  2583.59, 327.00, 0),
            ('2027-05-01', 2896.21,  2592.09, 301.12, 0),
            ('2027-06-01', 2898.89,  2600.63, 295.26, 0),
            ('2027-07-01', 2882.49,  2609.19, 270.30, 0),
            ('2027-08-01', 2884.09,  2617.78, 263.31, 0),
            ('2027-09-01', 2876.64,  2626.39, 247.25, 0),
            ('2027-10-01', 2861.73,  2635.04, 223.69, 0),
            ('2027-11-01', 2861.70,  2643.71, 214.99, 0),
            ('2027-12-01', 2847.77,  2652.41, 192.36, 0),
            ('2028-01-01', 2846.65,  2661.14, 182.51, 0),
            ('2028-02-01', 2839.09,  2669.90, 166.19, 0),
            ('2028-03-01', 2821.84,  2678.69, 140.15, 0),
            ('2028-04-01', 2823.89,  2687.51, 133.38, 0),
            ('2028-05-01', 2812.49,  2696.36, 113.13, 0),
            ('2028-06-01', 2808.60,  2705.23, 100.37, 0),
            ('2028-07-01', 2798.21,  2714.14,  81.07, 0),
            ('2028-08-01', 2793.20,  2723.07,  67.13, 0),
            ('2028-09-01', 2785.46,  2732.03,  50.43, 0),
            ('2028-10-01', 2776.62,  2741.03,  32.59, 0),
            ('2028-11-01', 2769.56,  2749.70,  16.86, 0),
        ]
        for data, rata, capitale, interessi, pagato in rate_sondrio:
            c.execute("""
                INSERT INTO rate_finanziamento
                    (finanziamento_id, data_scadenza, rata_totale, quota_capitale, quota_interessi, pagato)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (fin_id, data, rata, capitale, interessi, pagato))

    # Seed: Mutuo Banca Sella — inserisce solo se non già presente
    c.execute("SELECT id FROM finanziamenti WHERE nome='Mutuo - Banca Sella'")
    if not c.fetchone():
        c.execute("""
            INSERT INTO finanziamenti (nome, banca, importo_originale, data_inizio, data_fine, note)
            VALUES ('Mutuo - Banca Sella', 'Banca Sella', 13852.00,
                    '2022-07-29', '2026-07-29',
                    'Conto 8YB1667016750 - 48 rate mensili - Tasso 1,050%')
            RETURNING id
        """)
        fin_id = c.fetchone()[0]
        # pagato=1 per rate con scadenza <= 28/02/2026
        rate_sella = [
            ('2022-07-29', 287.71,   0.00,   287.71, 1),
            ('2022-08-29', 294.82, 282.70,    12.12, 1),
            ('2022-09-29', 294.82, 282.95,    11.87, 1),
            ('2022-10-29', 294.82, 283.20,    11.62, 1),
            ('2022-11-29', 294.82, 283.45,    11.37, 1),
            ('2022-12-29', 294.82, 283.70,    11.12, 1),
            ('2023-01-29', 294.82, 283.94,    10.88, 1),
            ('2023-02-28', 294.82, 284.19,    10.63, 1),
            ('2023-03-29', 294.82, 284.44,    10.38, 1),
            ('2023-04-29', 294.82, 284.69,    10.13, 1),
            ('2023-05-29', 294.82, 284.94,     9.88, 1),
            ('2023-06-29', 294.82, 285.19,     9.63, 1),
            ('2023-07-29', 294.82, 285.44,     9.38, 1),
            ('2023-08-29', 294.82, 285.69,     9.13, 1),
            ('2023-09-29', 294.82, 285.94,     8.88, 1),
            ('2023-10-29', 294.82, 286.19,     8.63, 1),
            ('2023-11-29', 294.82, 286.44,     8.38, 1),
            ('2023-12-29', 294.82, 286.69,     8.13, 1),
            ('2024-01-29', 294.82, 286.94,     7.88, 1),
            ('2024-02-29', 294.82, 287.19,     7.63, 1),
            ('2024-03-29', 294.82, 287.44,     7.38, 1),
            ('2024-04-29', 294.82, 287.69,     7.13, 1),
            ('2024-05-29', 294.82, 287.94,     6.88, 1),
            ('2024-06-29', 294.82, 288.20,     6.62, 1),
            ('2024-07-29', 294.82, 288.45,     6.37, 1),
            ('2024-08-29', 294.82, 288.70,     6.12, 1),
            ('2024-09-29', 294.82, 288.95,     5.87, 1),
            ('2024-10-29', 294.82, 289.21,     5.61, 1),
            ('2024-11-29', 294.82, 289.46,     5.36, 1),
            ('2024-12-29', 294.82, 289.71,     5.11, 1),
            ('2025-01-29', 294.82, 289.97,     4.85, 1),
            ('2025-02-28', 294.82, 290.22,     4.60, 1),
            ('2025-03-29', 294.82, 290.47,     4.35, 1),
            ('2025-04-29', 294.82, 290.73,     4.09, 1),
            ('2025-05-29', 294.82, 290.98,     3.84, 1),
            ('2025-06-29', 294.82, 291.24,     3.58, 1),
            ('2025-07-29', 294.82, 291.49,     3.33, 1),
            ('2025-08-29', 294.82, 291.75,     3.07, 1),
            ('2025-09-29', 294.82, 292.00,     2.82, 1),
            ('2025-10-29', 294.82, 292.26,     2.56, 1),
            ('2025-11-29', 294.82, 292.51,     2.31, 1),
            ('2025-12-29', 294.82, 292.77,     2.05, 1),
            ('2026-01-29', 294.82, 293.03,     1.79, 1),
            ('2026-02-28', 294.82, 293.28,     1.54, 1),
            ('2026-03-29', 294.82, 293.54,     1.28, 0),
            ('2026-04-29', 294.82, 293.80,     1.02, 0),
            ('2026-05-29', 294.82, 294.05,     0.77, 0),
            ('2026-06-29', 294.82, 294.31,     0.51, 0),
            ('2026-07-29', 294.82, 293.94,     0.88, 0),
        ]
        for data, rata, capitale, interessi, pagato in rate_sella:
            c.execute("""
                INSERT INTO rate_finanziamento
                    (finanziamento_id, data_scadenza, rata_totale, quota_capitale, quota_interessi, pagato)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (fin_id, data, rata, capitale, interessi, pagato))

    # Seed: Piano rientro Contributi INPS dipendenti aprile 2024
    c.execute("SELECT id FROM piani_rientro WHERE nome='Contributi INPS dipendenti - Aprile 2024'")
    if not c.fetchone():
        c.execute("""
            INSERT INTO piani_rientro (nome, tipo, ente, anno_riferimento, note)
            VALUES ('Contributi INPS dipendenti - Aprile 2024', 'contributi',
                    'INPS / Agenzia delle Entrate', 2024,
                    'Rateizzazione contributi dipendenti apr 2024 - 6 rate mensili ogni 29 del mese')
            RETURNING id
        """)
        pid = c.fetchone()[0]
        # (data_scadenza, importo_cassa, quota_interessi, pagato)
        # Jan-Mar 2026 già passati → pagato=1
        rate_inps = [
            ('2026-01-29', 120.47, 5.89, 1),
            ('2026-02-28', 120.47, 4.93, 1),
            ('2026-03-29', 120.47, 3.96, 1),
            ('2026-04-29', 120.47, 2.98, 0),
            ('2026-05-29', 120.47, 2.00, 0),
            ('2026-06-29', 120.47, 1.00, 0),
        ]
        for data, importo, interessi, pagato in rate_inps:
            c.execute("""
                INSERT INTO rate_piano_rientro
                    (piano_rientro_id, data_scadenza, importo, quota_interessi, pagato)
                VALUES (%s, %s, %s, %s, %s)
            """, (pid, data, importo, interessi, pagato))

    # Seed: Piano rientro IVA Novembre 2023
    c.execute("SELECT id FROM piani_rientro WHERE nome='IVA Novembre 2023'")
    if not c.fetchone():
        c.execute("""
            INSERT INTO piani_rientro (nome, tipo, ente, anno_riferimento, note)
            VALUES ('IVA Novembre 2023', 'iva',
                    'Agenzia delle Entrate', 2023,
                    'Rateizzazione IVA nov 2023 - rate 12-72 da 24/1/2026 a 24/1/2031 - 209,18 EUR/mese')
            RETURNING id
        """)
        pid = c.fetchone()[0]
        # (data_scadenza, importo_cassa, quota_interessi, pagato)
        # Rate 12-14 (gen/feb/mar 2026) già pagate → pagato=1
        rate_iva = [
            ('2026-01-24', 209.18, 32.90, 1),
            ('2026-02-24', 209.18, 32.42, 1),
            ('2026-03-24', 209.18, 31.94, 1),
            ('2026-04-24', 209.18, 31.45, 0),
            ('2026-05-24', 209.18, 30.97, 0),
            ('2026-06-24', 209.18, 30.48, 0),
            ('2026-07-24', 209.18, 29.99, 0),
            ('2026-08-24', 209.18, 29.50, 0),
            ('2026-09-24', 209.18, 29.00, 0),
            ('2026-10-24', 209.18, 28.51, 0),
            ('2026-11-24', 209.18, 28.01, 0),
            ('2026-12-24', 209.18, 27.51, 0),
            ('2027-01-24', 209.18, 27.01, 0),
            ('2027-02-24', 209.18, 26.51, 0),
            ('2027-03-24', 209.18, 26.00, 0),
            ('2027-04-24', 209.18, 25.49, 0),
            ('2027-05-24', 209.18, 24.99, 0),
            ('2027-06-24', 209.18, 24.47, 0),
            ('2027-07-24', 209.18, 23.96, 0),
            ('2027-08-24', 209.18, 23.45, 0),
            ('2027-09-24', 209.18, 22.93, 0),
            ('2027-10-24', 209.18, 22.41, 0),
            ('2027-11-24', 209.18, 21.89, 0),
            ('2027-12-24', 209.18, 21.37, 0),
            ('2028-01-24', 209.18, 20.85, 0),
            ('2028-02-24', 209.18, 20.32, 0),
            ('2028-03-24', 209.18, 19.79, 0),
            ('2028-04-24', 209.18, 19.26, 0),
            ('2028-05-24', 209.18, 18.73, 0),
            ('2028-06-24', 209.18, 18.19, 0),
            ('2028-07-24', 209.18, 17.66, 0),
            ('2028-08-24', 209.18, 17.12, 0),
            ('2028-09-24', 209.18, 16.58, 0),
            ('2028-10-24', 209.18, 16.04, 0),
            ('2028-11-24', 209.18, 15.49, 0),
            ('2028-12-24', 209.18, 14.95, 0),
            ('2029-01-24', 209.18, 14.40, 0),
            ('2029-02-24', 209.18, 13.85, 0),
            ('2029-03-24', 209.18, 13.30, 0),
            ('2029-04-24', 209.18, 12.74, 0),
            ('2029-05-24', 209.18, 12.18, 0),
            ('2029-06-24', 209.18, 11.63, 0),
            ('2029-07-24', 209.18, 11.06, 0),
            ('2029-08-24', 209.18, 10.50, 0),
            ('2029-09-24', 209.18,  9.94, 0),
            ('2029-10-24', 209.18,  9.37, 0),
            ('2029-11-24', 209.18,  8.80, 0),
            ('2029-12-24', 209.18,  8.23, 0),
            ('2030-01-24', 209.18,  7.66, 0),
            ('2030-02-24', 209.18,  7.08, 0),
            ('2030-03-24', 209.18,  6.50, 0),
            ('2030-04-24', 209.18,  5.92, 0),
            ('2030-05-24', 209.18,  5.34, 0),
            ('2030-06-24', 209.18,  4.76, 0),
            ('2030-07-24', 209.18,  4.17, 0),
            ('2030-08-24', 209.18,  3.58, 0),
            ('2030-09-24', 209.18,  2.99, 0),
            ('2030-10-24', 209.18,  2.40, 0),
            ('2030-11-24', 209.18,  1.80, 0),
            ('2030-12-24', 209.18,  1.20, 0),
            ('2031-01-24', 209.18,  0.60, 0),
        ]
        for data, importo, interessi, pagato in rate_iva:
            c.execute("""
                INSERT INTO rate_piano_rientro
                    (piano_rientro_id, data_scadenza, importo, quota_interessi, pagato)
                VALUES (%s, %s, %s, %s, %s)
            """, (pid, data, importo, interessi, pagato))

    # Seed: Piano rientro IVA Settembre 2023
    c.execute("SELECT id FROM piani_rientro WHERE nome='IVA Settembre 2023'")
    if not c.fetchone():
        c.execute("""
            INSERT INTO piani_rientro (nome, tipo, ente, anno_riferimento, note)
            VALUES ('IVA Settembre 2023', 'iva',
                    'Agenzia delle Entrate', 2023,
                    'Rateizzazione IVA set 2023 - rate trimestrali - rate 8-20 da mar 2026 a feb 2029')
            RETURNING id
        """)
        pid = c.fetchone()[0]
        # (data_scadenza, importo_cassa, quota_interessi, pagato)
        # Rata 8 scade 2/3/2026 → già passata → pagato=1
        rate_iva_set = [
            ('2026-03-02', 595.15, 34.31, 1),
            ('2026-06-01', 600.10, 39.26, 0),
            ('2026-08-31', 605.05, 44.21, 0),
            ('2026-11-30', 609.94, 49.10, 0),
            ('2027-03-01', 614.78, 53.94, 0),
            ('2027-05-31', 619.73, 58.89, 0),
            ('2027-08-31', 624.68, 63.84, 0),
            ('2027-11-30', 629.57, 68.73, 0),
            ('2028-02-29', 634.46, 73.62, 0),
            ('2028-05-31', 639.41, 78.57, 0),
            ('2028-08-31', 644.36, 83.52, 0),
            ('2028-11-30', 649.25, 88.41, 0),
            ('2029-02-28', 654.09, 93.25, 0),
        ]
        for data, importo, interessi, pagato in rate_iva_set:
            c.execute("""
                INSERT INTO rate_piano_rientro
                    (piano_rientro_id, data_scadenza, importo, quota_interessi, pagato)
                VALUES (%s, %s, %s, %s, %s)
            """, (pid, data, importo, interessi, pagato))

    # Seed: Piano rientro IVA Aprile/Maggio 2024
    c.execute("SELECT id FROM piani_rientro WHERE nome='IVA Aprile/Maggio 2024'")
    if not c.fetchone():
        c.execute("""
            INSERT INTO piani_rientro (nome, tipo, ente, anno_riferimento, note)
            VALUES ('IVA Aprile/Maggio 2024', 'iva',
                    'Agenzia delle Entrate', 2024,
                    'Rateizzazione IVA apr/mag 2024 - rate trimestrali - rate 5-20 da mar 2026 a dic 2029')
            RETURNING id
        """)
        pid = c.fetchone()[0]
        # (data_scadenza, importo_cassa, quota_interessi, pagato)
        # Rata 5 scade 31/3/2026 → non ancora passata → pagato=0
        rate_iva_apr = [
            ('2026-03-31', 938.93, 34.35, 0),
            ('2026-06-30', 946.82, 42.24, 0),
            ('2026-09-30', 954.80, 50.22, 0),
            ('2026-12-31', 962.78, 58.20, 0),
            ('2027-03-31', 970.59, 66.01, 0),
            ('2027-06-30', 978.48, 73.90, 0),
            ('2027-09-30', 986.46, 81.88, 0),
            ('2027-12-31', 994.44, 89.86, 0),
            ('2028-03-31', 1002.34, 97.76, 0),
            ('2028-06-30', 1010.23, 105.65, 0),
            ('2028-10-02', 1018.21, 113.63, 0),
            ('2029-01-02', 1026.19, 121.61, 0),
            ('2029-04-03', 1034.00, 129.42, 0),
            ('2029-07-02', 1041.89, 137.31, 0),
            ('2029-10-01', 1049.87, 145.29, 0),
            ('2029-12-31', 1057.85, 153.27, 0),
        ]
        for data, importo, interessi, pagato in rate_iva_apr:
            c.execute("""
                INSERT INTO rate_piano_rientro
                    (piano_rientro_id, data_scadenza, importo, quota_interessi, pagato)
                VALUES (%s, %s, %s, %s, %s)
            """, (pid, data, importo, interessi, pagato))

    conn.commit()
