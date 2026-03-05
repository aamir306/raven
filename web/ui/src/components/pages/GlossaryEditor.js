import React, { useState, useCallback, useEffect } from 'react';
import { Input, message } from 'antd';
import { BookOpen, Plus, Trash2, Edit3, Save, X, Search } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

export default function GlossaryEditor({ onClose }) {
  const [terms, setTerms] = useState([]);
  const [search, setSearch] = useState('');
  const [editingId, setEditingId] = useState(null);
  const [showAdd, setShowAdd] = useState(false);
  const [form, setForm] = useState({ term: '', definition: '', sql_fragment: '', synonyms: '', tables: '' });

  useEffect(() => {
    fetch(`${API_BASE}/api/admin/glossary`)
      .then(r => r.json())
      .then(d => setTerms(d.terms || []))
      .catch(() => {
        /* Load example terms for demo */
        setTerms([
          { id: 1, term: 'Active User', definition: 'User with at least one session in last 30 days',
            sql_fragment: "WHERE last_activity > CURRENT_DATE - INTERVAL '30' DAY",
            synonyms: ['engaged user', 'live user', 'DAU'], tables: ['gold.daily_active_users'] },
          { id: 2, term: 'Revenue', definition: 'Total payment amount for completed/paid orders',
            sql_fragment: "SUM(amount) WHERE status IN ('completed', 'paid')",
            synonyms: ['sales', 'GMV', 'earnings'], tables: ['gold.orders'] },
        ]);
      });
  }, []);

  const filteredTerms = terms.filter(t =>
    !search || t.term.toLowerCase().includes(search.toLowerCase()) ||
    t.definition.toLowerCase().includes(search.toLowerCase())
  );

  const handleSave = useCallback(async () => {
    const newTerm = {
      id: editingId || Date.now(),
      term: form.term.trim(),
      definition: form.definition.trim(),
      sql_fragment: form.sql_fragment.trim(),
      synonyms: form.synonyms.split(',').map(s => s.trim()).filter(Boolean),
      tables: form.tables.split(',').map(s => s.trim()).filter(Boolean),
    };

    if (!newTerm.term || !newTerm.definition) {
      message.warning('Term and definition are required');
      return;
    }

    try {
      const method = editingId ? 'PUT' : 'POST';
      const url = editingId
        ? `${API_BASE}/api/admin/glossary/${editingId}`
        : `${API_BASE}/api/admin/glossary`;
      await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newTerm),
      });
    } catch {
      /* Offline — just update local state */
    }

    setTerms(prev => {
      if (editingId) return prev.map(t => t.id === editingId ? newTerm : t);
      return [...prev, newTerm];
    });

    setForm({ term: '', definition: '', sql_fragment: '', synonyms: '', tables: '' });
    setEditingId(null);
    setShowAdd(false);
    message.success(editingId ? 'Term updated' : 'Term added');
  }, [form, editingId]);

  const handleEdit = (term) => {
    setForm({
      term: term.term,
      definition: term.definition,
      sql_fragment: term.sql_fragment || '',
      synonyms: (term.synonyms || []).join(', '),
      tables: (term.tables || []).join(', '),
    });
    setEditingId(term.id);
    setShowAdd(true);
  };

  const handleDelete = async (id) => {
    try {
      await fetch(`${API_BASE}/api/admin/glossary/${id}`, { method: 'DELETE' });
    } catch { /* offline */ }
    setTerms(prev => prev.filter(t => t.id !== id));
    message.success('Term deleted');
  };

  return (
    <div className="page-panel">
      <div className="page-panel-header">
        <h2><BookOpen size={20} /> Business Glossary</h2>
        <button className="btn-icon" onClick={onClose}><X size={18} /></button>
      </div>

      <div className="page-panel-body">
        <p className="page-description">
          Define business terms so RAVEN understands your domain language.
          Changes sync to the semantic model and improve query accuracy.
        </p>

        <div className="glossary-toolbar">
          <div className="glossary-search">
            <Search size={14} />
            <Input
              placeholder="Search terms..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              size="small"
              style={{ flex: 1 }}
              allowClear
            />
          </div>
          <button className="btn-primary-sm" onClick={() => { setShowAdd(true); setEditingId(null); setForm({ term: '', definition: '', sql_fragment: '', synonyms: '', tables: '' }); }}>
            <Plus size={14} /> Add Term
          </button>
        </div>

        {showAdd && (
          <div className="glossary-form">
            <Input
              placeholder="Term name (e.g., Active User)"
              value={form.term}
              onChange={e => setForm(f => ({ ...f, term: e.target.value }))}
              size="small"
            />
            <Input.TextArea
              placeholder="Definition"
              value={form.definition}
              onChange={e => setForm(f => ({ ...f, definition: e.target.value }))}
              rows={2}
              size="small"
            />
            <Input
              placeholder="SQL fragment (e.g., WHERE status = 'active')"
              value={form.sql_fragment}
              onChange={e => setForm(f => ({ ...f, sql_fragment: e.target.value }))}
              size="small"
            />
            <Input
              placeholder="Synonyms (comma-separated)"
              value={form.synonyms}
              onChange={e => setForm(f => ({ ...f, synonyms: e.target.value }))}
              size="small"
            />
            <Input
              placeholder="Related tables (comma-separated)"
              value={form.tables}
              onChange={e => setForm(f => ({ ...f, tables: e.target.value }))}
              size="small"
            />
            <div className="glossary-form-actions">
              <button className="btn-primary-sm" onClick={handleSave}>
                <Save size={13} /> {editingId ? 'Update' : 'Save'}
              </button>
              <button className="btn-secondary-sm" onClick={() => { setShowAdd(false); setEditingId(null); }}>
                Cancel
              </button>
            </div>
          </div>
        )}

        <div className="glossary-list">
          {filteredTerms.map(t => (
            <div key={t.id} className="glossary-item">
              <div className="glossary-item-header">
                <span className="glossary-item-term">{t.term}</span>
                <div className="glossary-item-actions">
                  <button className="btn-icon-sm" onClick={() => handleEdit(t)} title="Edit"><Edit3 size={13} /></button>
                  <button className="btn-icon-sm" onClick={() => handleDelete(t.id)} title="Delete"><Trash2 size={13} /></button>
                </div>
              </div>
              <div className="glossary-item-def">{t.definition}</div>
              {t.sql_fragment && (
                <code className="glossary-item-sql">{t.sql_fragment}</code>
              )}
              <div className="glossary-item-meta">
                {t.synonyms?.length > 0 && (
                  <span>Synonyms: {t.synonyms.join(', ')}</span>
                )}
                {t.tables?.length > 0 && (
                  <span>Tables: {t.tables.join(', ')}</span>
                )}
              </div>
            </div>
          ))}
          {filteredTerms.length === 0 && (
            <div className="empty-state">
              <p>{search ? `No terms matching "${search}"` : 'No glossary terms defined yet.'}</p>
            </div>
          )}
        </div>

        <div className="glossary-footer">
          {terms.length} terms defined
        </div>
      </div>
    </div>
  );
}
