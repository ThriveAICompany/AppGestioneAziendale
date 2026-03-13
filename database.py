import os
import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
except ImportError:
    pass


class PgConn:
    """Thin wrapper around psycopg2 connection that mimics sqlite3's conn.execute() API."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, params)
        return cur

    def cursor(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

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

    conn.commit()
