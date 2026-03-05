import React, { useState, useEffect, useMemo } from 'react';
import { X, Plus, Trash2, FileText, Save, Search, ChevronDown, ChevronRight, Loader } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

/**
 * FocusDocumentEditor — Create and edit Focus Documents.
 * Allows selecting tables, adding business rules, verified queries, and column notes.
 */
export default function FocusDocumentEditor({ onClose }) {
  const [documents, setDocuments] = useState([]);
  const [editing, setEditing] = useState(null); // null = list view, object = editing
  const [allTables, setAllTables] = useState([]);
  const [tableSearch, setTableSearch] = useState('');
  const [saving, setSaving] = useState(false);
  const [loading, setLoading] = useState(true);

  const loadDocuments = () => {
    setLoading(true);
    fetch(`${API_BASE}/api/focus/documents`)
      .then(r => r.json())
      .then(d => setDocuments(d.documents || []))
      .catch(() => setDocuments([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    loadDocuments();
    fetch(`${API_BASE}/api/schema/tables`)
      .then(r => r.json())
      .then(d => setAllTables((d.tables || []).map(t => t.table_name)))
      .catch(() => setAllTables([]));
  }, []);

  const newDocument = () => {
    setEditing({
      name: '',
      description: '',
      tables: [],
      business_rules: [],
      verified_queries: [],
      column_notes: {},
    });
  };

  const editDocument = (doc) => {
    setEditing({ ...doc });
  };

  const deleteDocument = async (docId) => {
    if (!window.confirm('Delete this focus document?')) return;
    await fetch(`${API_BASE}/api/focus/documents/${docId}`, { method: 'DELETE' });
    loadDocuments();
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const method = editing.id ? 'PUT' : 'POST';
      const url = editing.id
        ? `${API_BASE}/api/focus/documents/${editing.id}`
        : `${API_BASE}/api/focus/documents`;
      await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(editing),
      });
      setEditing(null);
      loadDocuments();
    } catch (e) {
      alert('Save failed: ' + e.message);
    } finally {
      setSaving(false);
    }
  };

  const toggleTable = (table) => {
    setEditing(prev => ({
      ...prev,
      tables: prev.tables.includes(table)
        ? prev.tables.filter(t => t !== table)
        : [...prev.tables, table],
    }));
  };

  const addRule = () => {
    setEditing(prev => ({
      ...prev,
      business_rules: [...prev.business_rules, { rule: '', sql_fragment: '' }],
    }));
  };

  const updateRule = (i, field, value) => {
    setEditing(prev => {
      const rules = [...prev.business_rules];
      rules[i] = { ...rules[i], [field]: value };
      return { ...prev, business_rules: rules };
    });
  };

  const removeRule = (i) => {
    setEditing(prev => ({
      ...prev,
      business_rules: prev.business_rules.filter((_, idx) => idx !== i),
    }));
  };

  const addQuery = () => {
    setEditing(prev => ({
      ...prev,
      verified_queries: [...prev.verified_queries, { question: '', sql: '' }],
    }));
  };

  const updateQuery = (i, field, value) => {
    setEditing(prev => {
      const queries = [...prev.verified_queries];
      queries[i] = { ...queries[i], [field]: value };
      return { ...prev, verified_queries: queries };
    });
  };

  const removeQuery = (i) => {
    setEditing(prev => ({
      ...prev,
      verified_queries: prev.verified_queries.filter((_, idx) => idx !== i),
    }));
  };

  const filteredTables = useMemo(() => {
    const q = tableSearch.toLowerCase();
    return allTables.filter(t => t.toLowerCase().includes(q));
  }, [allTables, tableSearch]);

  // ── List View ──
  if (!editing) {
    return (
      <div className="tool-page">
        <div className="tool-page-header">
          <h2><FileText size={18} /> Focus Documents</h2>
          <div style={{ display: 'flex', gap: 8 }}>
            <button className="btn-secondary" onClick={newDocument}><Plus size={14} /> New Document</button>
            <button className="btn-icon" onClick={onClose}><X size={18} /></button>
          </div>
        </div>
        <div className="tool-page-body">
          {loading ? (
            <div className="tool-page-loading">Loading...</div>
          ) : documents.length === 0 ? (
            <div className="tool-page-empty">
              <FileText size={32} style={{ opacity: 0.3 }} />
              <p>No focus documents yet.</p>
              <p style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                Focus documents scope RAVEN's context to specific tables and rules,
                boosting accuracy for domain-specific questions.
              </p>
              <button className="btn-secondary" onClick={newDocument}><Plus size={14} /> Create your first</button>
            </div>
          ) : (
            <div className="focus-doc-list">
              {documents.map(doc => (
                <div key={doc.id} className="focus-doc-card">
                  <div className="focus-doc-card-header">
                    <FileText size={16} />
                    <span className="focus-doc-card-name">{doc.name}</span>
                    <span className="badge badge-info" style={{ fontSize: 10 }}>
                      {doc.table_count || doc.tables?.length || 0} tables
                    </span>
                    {doc.type === 'auto_dashboard' && (
                      <span className="badge badge-info" style={{ fontSize: 10 }}>auto</span>
                    )}
                  </div>
                  {doc.description && (
                    <div className="focus-doc-card-desc">{doc.description}</div>
                  )}
                  <div className="focus-doc-card-actions">
                    <button className="btn-text" onClick={() => editDocument(doc)}>Edit</button>
                    <button className="btn-text" style={{ color: 'var(--error)' }} onClick={() => deleteDocument(doc.id)}>
                      <Trash2 size={11} /> Delete
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  }

  // ── Editor View ──
  return (
    <div className="tool-page">
      <div className="tool-page-header">
        <h2><FileText size={18} /> {editing.id ? 'Edit' : 'New'} Focus Document</h2>
        <button className="btn-icon" onClick={() => setEditing(null)}><X size={18} /></button>
      </div>
      <div className="tool-page-body focus-doc-editor">
        <label>
          Name
          <input
            type="text"
            value={editing.name}
            onChange={e => setEditing(prev => ({ ...prev, name: e.target.value }))}
            placeholder="e.g. Revenue & Payments"
          />
        </label>

        <label>
          Description
          <input
            type="text"
            value={editing.description}
            onChange={e => setEditing(prev => ({ ...prev, description: e.target.value }))}
            placeholder="What domain does this focus cover?"
          />
        </label>

        {/* Table selector */}
        <div className="focus-doc-section">
          <h4>Priority Tables ({editing.tables.length} selected)</h4>
          <div className="focus-doc-table-search">
            <Search size={13} />
            <input
              type="text"
              value={tableSearch}
              onChange={e => setTableSearch(e.target.value)}
              placeholder="Search tables..."
            />
          </div>
          <div className="focus-doc-table-list">
            {filteredTables.slice(0, 50).map(table => (
              <label key={table} className="focus-doc-table-item">
                <input
                  type="checkbox"
                  checked={editing.tables.includes(table)}
                  onChange={() => toggleTable(table)}
                />
                <span>{table}</span>
              </label>
            ))}
            {filteredTables.length > 50 && (
              <div className="focus-doc-table-more">
                {filteredTables.length - 50} more tables — refine your search
              </div>
            )}
          </div>
        </div>

        {/* Business rules */}
        <div className="focus-doc-section">
          <div className="focus-doc-section-header">
            <h4>Business Rules ({editing.business_rules.length})</h4>
            <button className="btn-text" onClick={addRule}><Plus size={12} /> Add</button>
          </div>
          {editing.business_rules.map((rule, i) => (
            <div key={i} className="focus-doc-rule-row">
              <input
                type="text"
                value={rule.rule || ''}
                onChange={e => updateRule(i, 'rule', e.target.value)}
                placeholder="e.g. Revenue = SUM(amount) WHERE status IN ('completed','paid')"
              />
              <input
                type="text"
                value={rule.sql_fragment || ''}
                onChange={e => updateRule(i, 'sql_fragment', e.target.value)}
                placeholder="SQL fragment (optional)"
              />
              <button className="btn-icon" onClick={() => removeRule(i)}><Trash2 size={12} /></button>
            </div>
          ))}
        </div>

        {/* Verified queries */}
        <div className="focus-doc-section">
          <div className="focus-doc-section-header">
            <h4>Verified Queries ({editing.verified_queries.length})</h4>
            <button className="btn-text" onClick={addQuery}><Plus size={12} /> Add</button>
          </div>
          {editing.verified_queries.map((q, i) => (
            <div key={i} className="focus-doc-rule-row">
              <input
                type="text"
                value={q.question || ''}
                onChange={e => updateQuery(i, 'question', e.target.value)}
                placeholder="Natural language question"
              />
              <input
                type="text"
                value={q.sql || ''}
                onChange={e => updateQuery(i, 'sql', e.target.value)}
                placeholder="Verified SQL"
              />
              <button className="btn-icon" onClick={() => removeQuery(i)}><Trash2 size={12} /></button>
            </div>
          ))}
        </div>

        <div className="focus-doc-save-bar">
          <button className="btn-secondary" onClick={() => setEditing(null)}>Cancel</button>
          <button
            className="metabase-push-submit"
            onClick={handleSave}
            disabled={saving || !editing.name.trim() || editing.tables.length === 0}
          >
            {saving ? <><Loader size={14} className="spin" /> Saving...</> : <><Save size={14} /> Save Document</>}
          </button>
        </div>
      </div>
    </div>
  );
}
