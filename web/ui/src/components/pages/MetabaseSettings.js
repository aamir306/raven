import React, { useState, useEffect } from 'react';
import { X, Wifi, WifiOff, Key, Link, Database, FolderOpen, Save, Loader } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

/**
 * MetabaseSettings — Configuration page for Metabase integration.
 * Shows connection status, auth settings, and default collection/database.
 */
export default function MetabaseSettings({ onClose }) {
  const [config, setConfig] = useState(null);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE}/api/metabase/config`)
      .then(r => r.json())
      .then(setConfig)
      .catch(() => setConfig({ url: '', has_api_key: false, has_session_id: false, database_id: 1, collection_name: 'RAVEN Generated' }))
      .finally(() => setLoading(false));
  }, []);

  const handleTestConnection = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const resp = await fetch(`${API_BASE}/api/metabase/test-connection`, { method: 'POST' });
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
            <div className="settings-section">
              <h3>Connection</h3>
              <div className="settings-row">
                <span className="settings-label">Metabase URL</span>
                <span className="settings-value">
                  {config?.url || <em className="text-muted">Not configured — set METABASE_URL env var</em>}
                </span>
              </div>
              <div className="settings-row">
                <span className="settings-label">Authentication</span>
                <span className="settings-value">
                  {config?.has_api_key ? (
                    <span className="badge badge-verified"><Key size={11} /> API Key configured</span>
                  ) : config?.has_session_id ? (
                    <span className="badge badge-info"><Key size={11} /> Session ID configured</span>
                  ) : (
                    <span className="text-muted">No auth — set METABASE_API_KEY or METABASE_SESSION_ID</span>
                  )}
                </span>
              </div>
              <div className="settings-row">
                <span className="settings-label">Default Database ID</span>
                <span className="settings-value">{config?.database_id || 1}</span>
              </div>
              <div className="settings-row">
                <span className="settings-label">Default Collection</span>
                <span className="settings-value">{config?.collection_name || 'RAVEN Generated'}</span>
              </div>
            </div>

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

            <div className="settings-section">
              <h3>Setup Instructions</h3>
              <div className="settings-instructions">
                <p>Add the following environment variables to your <code>.env</code> file:</p>
                <pre>{`METABASE_URL=https://your-metabase-instance.com
METABASE_API_KEY=mb_xxxxxxxxxxxxxxxx
# OR
METABASE_SESSION_ID=xxxxxxxx-xxxx-xxxx-xxxx
METABASE_DATABASE_ID=1
METABASE_COLLECTION=RAVEN Generated`}</pre>
                <p style={{ marginTop: 8, fontSize: 11, color: 'var(--text-muted)' }}>
                  API Key: Metabase → Admin → Settings → Authentication → API Keys<br />
                  Session ID: Browser DevTools → Application → Cookies → metabase.SESSION
                </p>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
