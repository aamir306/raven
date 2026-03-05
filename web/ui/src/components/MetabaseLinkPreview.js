import React from 'react';
import { BarChart3, FileQuestion, X, Loader, ExternalLink, Database, Layout } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

// Metabase URL regex — matches dashboard, question, collection links
const METABASE_URL_RE = /https?:\/\/[^\s/]+\/(dashboard|question)\/(\d+)/i;

/**
 * Detect a Metabase URL in free text. Returns { type, id, url } or null.
 */
export function detectMetabaseUrl(text) {
  const match = text.match(METABASE_URL_RE);
  if (!match) return null;
  return {
    type: match[1].toLowerCase(),       // 'dashboard' | 'question'
    id: parseInt(match[2], 10),
    url: match[0],
  };
}

/**
 * Fetch preview metadata from backend.
 */
export async function fetchLinkPreview(url) {
  const resp = await fetch(`${API_BASE}/api/metabase/preview-link`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url }),
  });
  if (!resp.ok) return null;
  return resp.json();
}

/**
 * Inline preview card rendered above the input bar when a Metabase URL is detected.
 */
export default function MetabaseLinkPreview({ preview, loading, onRemove }) {
  if (!preview && !loading) return null;

  if (loading) {
    return (
      <div className="metabase-link-preview loading">
        <Loader size={14} className="spin" />
        <span>Loading Metabase preview…</span>
      </div>
    );
  }

  const icon = preview.type === 'dashboard'
    ? <Layout size={16} />
    : <FileQuestion size={16} />;

  return (
    <div className="metabase-link-preview">
      <div className="metabase-link-preview-icon">{icon}</div>
      <div className="metabase-link-preview-info">
        <div className="metabase-link-preview-name">
          {preview.name || `${preview.type} #${preview.id}`}
        </div>
        <div className="metabase-link-preview-meta">
          {preview.type === 'dashboard' ? (
            <>
              <span>{preview.card_count || 0} cards</span>
              <span>·</span>
              <span><Database size={10} /> {preview.table_count || 0} tables</span>
              {preview.owner && (<><span>·</span><span>{preview.owner}</span></>)}
            </>
          ) : (
            <>
              <span><BarChart3 size={10} /> {preview.display || 'table'}</span>
              {preview.tables?.length > 0 && (
                <><span>·</span><span><Database size={10} /> {preview.tables.length} tables</span></>
              )}
            </>
          )}
        </div>
        <div className="metabase-link-preview-hint">
          Focus will auto-activate when you send — priority tables boosted 5×
        </div>
      </div>
      <div className="metabase-link-preview-actions">
        <a
          href={`#`}
          className="metabase-link-preview-open"
          onClick={(e) => { e.preventDefault(); window.open(preview._url, '_blank'); }}
          title="Open in Metabase"
        >
          <ExternalLink size={12} />
        </a>
        <button className="metabase-link-preview-remove" onClick={onRemove} title="Remove link">
          <X size={12} />
        </button>
      </div>
    </div>
  );
}
