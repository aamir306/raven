import React, { useState, useEffect } from 'react';
import { X, Wifi, WifiOff, Key, Link, Database, FolderOpen, Save, Loader } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

// Keys for localStorage
const LS_SESSION_ID = 'raven_mb_session_id';
const LS_DATABASE_ID = 'raven_mb_database_id';
const LS_COLLECTION = 'raven_mb_collection';

/**
 * Read Metabase browser overrides from localStorage.
 * Used by MetabasePushModal and any component that calls Metabase APIs.
 */
export function getMetabaseBrowserConfig() {
  return {
    _mb_session_id: localStorage.getItem(LS_SESSION_ID) || '',
    _mb_database_id: localStorage.getItem(LS_DATABASE_ID) || '',
    _mb_collection_name: localStorage.getItem(LS_COLLECTION) || '',
  };
}

/**
 * MetabaseSettings — Configuration page for Metabase integration.
 * Server config (URL, API key) from env. User config (session, DB, collection) from browser.
 */
export default function MetabaseSettings({ onClose }) {
  const [config, setConfig] = useState(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saved, setSaved] = useState(false);

  // Browser-side config
  const [sessionId, setSessionId] = useState(() => localStorage.getItem(LS_SESSION_ID) || '');
  const [databaseId, setDatabaseId] = useState(() => localStorage.getItem(LS_DATABASE_ID) || '');
  const [collectionName, setCollectionName] = useState(() => localStorage.getItem(LS_COLLECTION) || '');

  useEffect(() => {
    fetch(`${API_BASE}/api/metabase/config`)
      .then(r => r.json())
      .then(setConfig)
      .catch(() => setConfig({ url: '', has_api_key: false, has_session_id: false, database_id: 0, collection_name: 'RAVEN Generated' }))
      .finally(() => setLoading(false));
  }, []);

  const handleSaveBrowserConfig = () => {
    if (sessionId) localStorage.setItem(LS_SESSION_ID, sessionId);
    else localStorage.removeItem(LS_SESSION_ID);
    if (databaseId) localStorage.setItem(LS_DATABASE_ID, databaseId);
    else localStorage.removeItem(LS_DATABASE_ID);
    if (collectionName) localStorage.setItem(LS_COLLECTION, collectionName);
    else localStorage.removeItem(LS_COLLECTION);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  const handleTestConnection = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const resp = await fetch(`${API_BASE}/api/metabase/test-connection`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          _mb_session_id: sessionId || undefined,
        }),
      });
      setTestResult(await resp.json());
    } catch (e) {
      setTestResult({ connected: false, error: e.message });
    } finally {
      setTesting(false);
    }
  };

  return (
    <div className="tool-page">
      <div className="tool-page-header">
        <h2><Link size={18} /> Metabase Integration</h2>
        <button className="btn-icon" onClick={onClose}><X size={18} /></button>
      </div>

      <div className="tool-page-body">
        {loading ? (
          <div className="tool-page-loading">Loading configuration...</div>
        ) : (
          <>
            {/* Server-side config (read-only, from env) */}
            <div className="settings-section">
              <h3>Server Configuration</h3>
              <p style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8 }}>
                Set in <code>.env</code> on the server. Restart required after changes.
              </p>
              <div className="settings-row">
                <span className="settings-label">Metabase URL</span>
                <span className="settings-value">
                  {config?.url || <em className="text-muted">Not configured</em>}
                </span>
              </div>
              <div className="settings-row">
                <span className="settings-label">API Key</span>
                <span className="settings-value">
                  {config?.has_api_key ? (
                    <span className="badge badge-verified"><Key size={11} /> Configured</span>
                  ) : (
                    <span className="text-muted">Not set</span>
                  )}
                </span>
              </div>
            </div>

            {/* Browser-side config (editable, saved to localStorage) */}
            <div className="settings-section">
              <h3>Your Settings</h3>
              <p style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8 }}>
                Saved in your browser only. Not sent to the server until you push to Metabase.
              </p>

              <label className="settings-field">
                <span className="settings-field-label"><Key size={12} /> Session ID</span>
                <input
                  type="password"
                  value={sessionId}
                  onChange={e => setSessionId(e.target.value)}
                  placeholder="xxxxxxxx-xxxx-xxxx-xxxx (from Metabase cookie)"
                  className="settings-input"
                />
                <span className="settings-field-hint">
                  DevTools → Application → Cookies → metabase.SESSION. Used if no API Key is set on server.
                </span>
              </label>

              <label className="settings-field">
                <span className="settings-field-label"><Database size={12} /> Database ID</span>
                <input
                  type="number"
                  value={databaseId}
                  onChange={e => setDatabaseId(e.target.value)}
                  placeholder="Optional — auto-detected from dashboard links"
                  className="settings-input"
                  min="1"
                />
                <span className="settings-field-hint">
                  Optional. If blank, RAVEN auto-detects from dashboard link or falls back to server default.
                </span>
              </label>

              <label className="settings-field">
                <span className="settings-field-label"><FolderOpen size={12} /> Collection Name</span>
                <input
                  type="text"
                  value={collectionName}
                  onChange={e => setCollectionName(e.target.value)}
                  placeholder="RAVEN Generated (default)"
                  className="settings-input"
                />
                <span className="settings-field-hint">
                  Optional. Where pushed questions are saved. Leave blank for "RAVEN Generated".
                </span>
              </label>

              <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 8 }}>
                <button className="btn-primary-sm" onClick={handleSaveBrowserConfig}>
                  <Save size={12} /> Save to Browser
                </button>
                {saved && <span style={{ fontSize: 11, color: 'var(--success)' }}>✓ Saved</span>}
              </div>
            </div>

            {/* Test connection */}
            <div className="settings-section">
              <h3>Test Connection</h3>
              <button
                className="btn-secondary"
                onClick={handleTestConnection}
                disabled={testing || !config?.url}
                style={{ marginBottom: 12 }}
              >
                {testing ? <><Loader size={14} className="spin" /> Testing...</> : <><Wifi size={14} /> Test Connection</>}
              </button>
              {testResult && (
                <div className={`settings-test-result ${testResult.connected ? 'success' : 'error'}`}>
                  {testResult.connected ? (
                    <>
                      <Wifi size={14} />
                      <span>Connected! User: {testResult.user} · {testResult.dashboards} dashboards</span>
                    </>
                  ) : (
                    <>
                      <WifiOff size={14} />
                      <span>Connection failed: {testResult.error}</span>
                    </>
                  )}
                </div>
              )}
            </div>

            {/* Setup instructions */}
            <div className="settings-section">
              <h3>Setup Instructions</h3>
              <div className="settings-instructions">
                <p>Add to your server <code>.env</code> file:</p>
                <pre>{`METABASE_URL=https://metabase-prod.penpencil.co
METABASE_API_KEY=mb_xxxxxxxxxxxxxxxx`}</pre>
                <p style={{ marginTop: 8, fontSize: 11, color: 'var(--text-muted)' }}>
                  API Key: Metabase → Admin → Settings → Authentication → API Keys<br />
                  Session ID and Database ID can be set here in the browser instead.
                </p>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
