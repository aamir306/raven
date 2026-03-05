import React, { useState, useEffect, useRef, useMemo } from 'react';
import { Search, FileText, BarChart3, Globe, X, Target } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

/**
 * FocusDropdown — Appears when user types "/" in the input bar.
 * Shows available Focus Documents and Metabase Dashboards.
 * Selecting one activates Focus Mode scoping (tiered, not hard-blocked).
 */
export default function FocusDropdown({ visible, onSelect, onClose }) {
  const [documents, setDocuments] = useState([]);
  const [dashboards, setDashboards] = useState([]);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(false);
  const searchRef = useRef(null);

  useEffect(() => {
    if (visible) {
      setLoading(true);
      Promise.all([
        fetch(`${API_BASE}/api/focus/documents`).then(r => r.json()).catch(() => ({ documents: [] })),
        fetch(`${API_BASE}/api/metabase/dashboards`).then(r => r.json()).catch(() => ({ dashboards: [] })),
      ]).then(([docData, mbData]) => {
        setDocuments(docData.documents || []);
        setDashboards(mbData.dashboards || []);
      }).finally(() => setLoading(false));
      setTimeout(() => searchRef.current?.focus(), 100);
    } else {
      setSearch('');
    }
  }, [visible]);

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    const filteredDocs = documents.filter(d =>
      d.name?.toLowerCase().includes(q) || d.description?.toLowerCase().includes(q)
    );
    const filteredDash = dashboards.filter(d =>
      d.name?.toLowerCase().includes(q) || d.description?.toLowerCase().includes(q)
    );
    return { docs: filteredDocs, dash: filteredDash };
  }, [documents, dashboards, search]);

  if (!visible) return null;

  return (
    <div className="focus-dropdown-overlay" onClick={onClose}>
      <div className="focus-dropdown" onClick={e => e.stopPropagation()}>
        <div className="focus-dropdown-header">
          <Search size={14} />
          <input
            ref={searchRef}
            type="text"
            placeholder="Search focuses..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="focus-dropdown-search"
          />
          <button className="focus-dropdown-close" onClick={onClose}>
            <X size={14} />
          </button>
        </div>

        <div className="focus-dropdown-body">
          {loading && <div className="focus-dropdown-loading">Loading...</div>}

          {!loading && filtered.docs.length > 0 && (
            <>
              <div className="focus-dropdown-section-label">
                <FileText size={12} /> DOCUMENTS
              </div>
              {filtered.docs.map(doc => (
                <button
                  key={doc.id}
                  className="focus-dropdown-item"
                  onClick={() => { onSelect({ type: 'document', ...doc }); onClose(); }}
                >
                  <div className="focus-dropdown-item-icon"><FileText size={16} /></div>
                  <div className="focus-dropdown-item-info">
                    <div className="focus-dropdown-item-name">{doc.name}</div>
                    <div className="focus-dropdown-item-meta">
                      {doc.table_count || doc.tables?.length || 0} tables
                      {doc.business_rules?.length ? ` · ${doc.business_rules.length} rules` : ''}
                      {doc.verified_queries?.length ? ` · ${doc.verified_queries.length} verified queries` : ''}
                    </div>
                    {doc.description && (
                      <div className="focus-dropdown-item-desc">{doc.description}</div>
                    )}
                  </div>
                </button>
              ))}
            </>
          )}

          {!loading && filtered.dash.length > 0 && (
            <>
              <div className="focus-dropdown-section-label">
                <BarChart3 size={12} /> METABASE DASHBOARDS
              </div>
              {filtered.dash.map(dash => (
                <button
                  key={dash.id}
                  className="focus-dropdown-item"
                  onClick={() => { onSelect({ type: 'dashboard', ...dash }); onClose(); }}
                >
                  <div className="focus-dropdown-item-icon"><BarChart3 size={16} /></div>
                  <div className="focus-dropdown-item-info">
                    <div className="focus-dropdown-item-name">{dash.name}</div>
                    <div className="focus-dropdown-item-meta">
                      {dash.card_count ? `${dash.card_count} cards` : ''}
                      {dash.description ? ` · ${dash.description.slice(0, 60)}` : ''}
                    </div>
                  </div>
                </button>
              ))}
            </>
          )}

          {!loading && filtered.docs.length === 0 && filtered.dash.length === 0 && (
            <div className="focus-dropdown-empty">
              {search ? `No focuses match "${search}"` : 'No focus documents yet. Create one from the sidebar.'}
            </div>
          )}

          <div className="focus-dropdown-divider" />
          <button
            className="focus-dropdown-item focus-dropdown-item-global"
            onClick={() => { onSelect(null); onClose(); }}
          >
            <div className="focus-dropdown-item-icon"><Globe size={16} /></div>
            <div className="focus-dropdown-item-info">
              <div className="focus-dropdown-item-name">No Focus (search all tables)</div>
              <div className="focus-dropdown-item-meta">Full 1,200+ table context — no priority boosting</div>
            </div>
          </button>
        </div>
      </div>
    </div>
  );
}
