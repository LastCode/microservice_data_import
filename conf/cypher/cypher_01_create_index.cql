// ============================================================
// Neo4j Database Configuration & Index Setup
// ============================================================

// Set transaction timeout to 100 minutes (for large imports)
CALL dbms.setConfigValue('db.transaction.timeout', '100m');

// ============================================================
// Transaction Node Indexes
// ============================================================
CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.transaction_id);
CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.cagid);
CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.gfcid);
CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.netting_id);
CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.cob_date);

// ============================================================
// Summary Node Indexes
// ============================================================
CREATE INDEX IF NOT EXISTS FOR (s:Summary_CAGID) ON (s.cagid);
CREATE INDEX IF NOT EXISTS FOR (s:Summary_CAGID) ON (s.cob_date);
CREATE INDEX IF NOT EXISTS FOR (s:Summary_GFCID) ON (s.gfcid);
CREATE INDEX IF NOT EXISTS FOR (s:Summary_NETTINGID) ON (s.netting_id);

// ============================================================
// Constraints (Unique Keys)
// ============================================================
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Summary_GFCID) REQUIRE n.gfcid IS UNIQUE;
