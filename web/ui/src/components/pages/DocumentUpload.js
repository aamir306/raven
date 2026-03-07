import React, { useState, useCallback, useEffect } from 'react';
import { Upload, message } from 'antd';
import { FileUp, File, Trash2, X, Loader } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

const ACCEPTED_TYPES = '.docx,.pdf,.md,.txt,.yaml,.yml';
const DOC_KIND_OPTIONS = [
  ['reference', 'Reference'],
  ['prd', 'PRD'],
  ['metric_spec', 'Metric Spec'],
  ['table_relation', 'Table Relation'],
  ['business_rule', 'Business Rule'],
  ['glossary_note', 'Glossary Note'],
  ['dashboard_note', 'Dashboard Note'],
  ['runbook', 'Runbook'],
  ['general', 'General'],
  ['other', 'Other'],
];
const TRUST_LEVEL_OPTIONS = [
  ['reference', 'Reference'],
  ['reviewed', 'Reviewed'],
  ['canonical', 'Canonical'],
];

function normalizeDocument(doc) {
  return {
    id: doc.focus_document_id || doc.filename,
    filename: doc.filename,
    title: doc.title || doc.filename,
    description: doc.description || '',
    docKind: doc.doc_kind || 'reference',
    domain: doc.domain || '',
    owner: doc.owner || '',
    trustLevel: doc.trust_level || 'reference',
    relatedTables: doc.related_tables || [],
    relatedMetrics: doc.related_metrics || [],
    tags: doc.tags || [],
    version: doc.version || '',
    effectiveDate: doc.effective_date || '',
    deprecated: !!doc.deprecated,
    chunks: doc.chunks || 0,
    date: doc.uploaded_at
      ? new Date(doc.uploaded_at * 1000).toLocaleDateString()
      : new Date().toLocaleDateString(),
    status: doc.status || 'uploaded',
    size: doc.size_bytes || 0,
  };
}

const DEFAULT_FORM = {
  title: '',
  description: '',
  doc_kind: 'reference',
  domain: '',
  owner: '',
  trust_level: 'reference',
  related_tables: '',
  related_metrics: '',
  tags: '',
  version: '',
  effective_date: '',
  deprecated: false,
};

export default function DocumentUpload({ onClose }) {
  const [documents, setDocuments] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [loadingDocs, setLoadingDocs] = useState(true);
  const [form, setForm] = useState(DEFAULT_FORM);

  const loadDocuments = useCallback(async () => {
    setLoadingDocs(true);
    try {
      const resp = await fetch(`${API_BASE}/api/admin/uploaded-docs`);
      if (resp.ok) {
        const data = await resp.json();
        setDocuments((data.documents || []).map(normalizeDocument));
      }
    } catch {
      setDocuments([]);
    } finally {
      setLoadingDocs(false);
    }
  }, []);

  useEffect(() => {
    loadDocuments();
  }, [loadDocuments]);

  const updateForm = useCallback((field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
  }, []);

  const buildUploadData = useCallback(() => ({
    title: form.title,
    description: form.description,
    doc_kind: form.doc_kind,
    domain: form.domain,
    owner: form.owner,
    trust_level: form.trust_level,
    related_tables: form.related_tables,
    related_metrics: form.related_metrics,
    tags: form.tags,
    version: form.version,
    effective_date: form.effective_date,
    deprecated: String(form.deprecated),
  }), [form]);

  const handleUpload = useCallback(async (info) => {
    const { file } = info;
    if (file.status === 'uploading') {
      setUploading(true);
      return;
    }

    setUploading(false);

    if (file.status === 'done') {
      const resp = file.response || {};
      const document = normalizeDocument({
        filename: resp.filename || file.name,
        title: resp.title || file.name,
        description: resp.description || '',
        doc_kind: resp.doc_kind || form.doc_kind,
        domain: resp.domain || form.domain,
        owner: resp.owner || form.owner,
        trust_level: resp.trust_level || form.trust_level,
        related_tables: resp.related_tables || [],
        related_metrics: resp.related_metrics || [],
        tags: resp.tags || [],
        version: resp.version || '',
        effective_date: resp.effective_date || '',
        deprecated: resp.deprecated || false,
        chunks: resp.chunks_created || 0,
        status: resp.status || 'uploaded',
        focus_document_id: resp.focus_document_id || null,
      });
      setDocuments(prev => {
        const remaining = prev.filter(d => d.filename !== document.filename);
        return [document, ...remaining];
      });
      message.success(
        `${file.name} uploaded - ${resp.chunks_created || 0} chunks indexed`,
      );
    } else if (file.status === 'error') {
      message.error(`${file.name} upload failed`);
    }
  }, [form]);

  const handleDelete = async (doc) => {
    try {
      const resp = await fetch(
        `${API_BASE}/api/admin/uploaded-docs/${encodeURIComponent(doc.filename)}`,
        { method: 'DELETE' },
      );
      if (resp.ok) {
        setDocuments(prev => prev.filter(d => d.filename !== doc.filename));
        message.success(`${doc.filename} deleted`);
      } else {
        message.error('Delete failed');
      }
    } catch {
      message.error('Delete failed');
    }
  };

  return (
    <div className="page-panel">
      <div className="page-panel-header">
        <h2><FileUp size={20} /> Document Management</h2>
        <button className="btn-icon" onClick={onClose}><X size={18} /></button>
      </div>

      <div className="page-panel-body">
        <p className="page-description">
          Add PRDs, metric specs, business rules, and table notes with structured
          metadata so RAVEN can retrieve them as higher-signal documentation context.
        </p>

        <div className="document-upload-form metabase-push-form">
          <div className="document-upload-grid">
            <label className="document-upload-wide">
              Title
              <input
                type="text"
                value={form.title}
                onChange={e => updateForm('title', e.target.value)}
                placeholder="Revenue PRD, Order Status Rules, Enrollment Definitions..."
              />
            </label>

            <label className="document-upload-wide">
              Description
              <input
                type="text"
                value={form.description}
                onChange={e => updateForm('description', e.target.value)}
                placeholder="Short summary of what this document should teach RAVEN"
              />
            </label>

            <label>
              Doc Type
              <select
                value={form.doc_kind}
                onChange={e => updateForm('doc_kind', e.target.value)}
              >
                {DOC_KIND_OPTIONS.map(([value, label]) => (
                  <option key={value} value={value}>{label}</option>
                ))}
              </select>
            </label>

            <label>
              Trust Level
              <select
                value={form.trust_level}
                onChange={e => updateForm('trust_level', e.target.value)}
              >
                {TRUST_LEVEL_OPTIONS.map(([value, label]) => (
                  <option key={value} value={value}>{label}</option>
                ))}
              </select>
            </label>

            <label>
              Domain
              <input
                type="text"
                value={form.domain}
                onChange={e => updateForm('domain', e.target.value)}
                placeholder="revenue, enrollment, finance..."
              />
            </label>

            <label>
              Owner
              <input
                type="text"
                value={form.owner}
                onChange={e => updateForm('owner', e.target.value)}
                placeholder="team, person, function..."
              />
            </label>

            <label className="document-upload-wide">
              Related Tables
              <input
                type="text"
                value={form.related_tables}
                onChange={e => updateForm('related_tables', e.target.value)}
                placeholder="analytics.orders, marts.revenue_daily"
              />
            </label>

            <label className="document-upload-wide">
              Related Metrics
              <input
                type="text"
                value={form.related_metrics}
                onChange={e => updateForm('related_metrics', e.target.value)}
                placeholder="net_revenue, order_count, active_enrollments"
              />
            </label>

            <label className="document-upload-wide">
              Tags
              <input
                type="text"
                value={form.tags}
                onChange={e => updateForm('tags', e.target.value)}
                placeholder="finance, monthly_close, approved"
              />
            </label>

            <label>
              Version
              <input
                type="text"
                value={form.version}
                onChange={e => updateForm('version', e.target.value)}
                placeholder="v1, 2026-Q1..."
              />
            </label>

            <label>
              Effective Date
              <input
                type="text"
                value={form.effective_date}
                onChange={e => updateForm('effective_date', e.target.value)}
                placeholder="2026-03-07"
              />
            </label>

            <label className="document-upload-checkbox">
              <input
                type="checkbox"
                checked={form.deprecated}
                onChange={e => updateForm('deprecated', e.target.checked)}
              />
              <span>Mark as deprecated</span>
            </label>
          </div>
        </div>

        <Upload.Dragger
          name="file"
          accept={ACCEPTED_TYPES}
          action={`${API_BASE}/api/admin/upload-doc`}
          data={buildUploadData}
          onChange={handleUpload}
          showUploadList={false}
          disabled={uploading}
          className="upload-dragger"
        >
          <div className="upload-zone">
            <FileUp size={36} className="upload-icon" />
            <p className="upload-text">Drop files here or click to browse</p>
            <p className="upload-hint">
              Supported: .docx, .pdf, .md, .txt, .yaml - metadata above will be stored with the document
            </p>
          </div>
        </Upload.Dragger>

        {loadingDocs ? (
          <div className="empty-state">
            <Loader size={20} className="spin" /> Loading documents...
          </div>
        ) : documents.length > 0 ? (
          <div className="doc-list">
            <h3 className="doc-list-title">Uploaded Documents</h3>
            {documents.map(doc => (
              <div key={doc.id} className="doc-item">
                <File size={16} className="doc-item-icon" />
                <div className="doc-item-info">
                  <span className="doc-item-name">{doc.title}</span>
                  <span className="doc-item-meta">
                    {doc.filename} - {doc.chunks} chunks indexed - {doc.date}
                  </span>
                  {doc.description && (
                    <div className="doc-item-desc">{doc.description}</div>
                  )}
                  <div className="doc-item-badges">
                    <span className="badge badge-info">{doc.docKind.replace(/_/g, ' ')}</span>
                    <span className="badge badge-info">{doc.trustLevel}</span>
                    {doc.domain && <span className="badge badge-info">{doc.domain}</span>}
                    {doc.version && <span className="badge badge-info">{doc.version}</span>}
                    {doc.deprecated && <span className="badge badge-info">deprecated</span>}
                  </div>
                </div>
                <div className="doc-item-actions">
                  <button
                    className="btn-icon-sm"
                    onClick={() => handleDelete(doc)}
                    title="Delete document"
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="empty-state">
            <p>No documents uploaded yet. Add PRDs, metric specs, and business rules to improve RAVEN's retrieval quality.</p>
          </div>
        )}
      </div>
    </div>
  );
}
