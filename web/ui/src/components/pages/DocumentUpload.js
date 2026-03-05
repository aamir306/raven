import React, { useState, useCallback, useEffect } from 'react';
import { Upload, message } from 'antd';
import { FileUp, File, Trash2, X, Loader } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

const ACCEPTED_TYPES = '.docx,.pdf,.md,.txt,.yaml,.yml';

export default function DocumentUpload({ onClose }) {
  const [documents, setDocuments] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [loadingDocs, setLoadingDocs] = useState(true);

  // Fetch existing uploaded docs on mount
  useEffect(() => {
    (async () => {
      try {
        const resp = await fetch(`${API_BASE}/api/admin/uploaded-docs`);
        if (resp.ok) {
          const data = await resp.json();
          setDocuments((data.documents || []).map(d => ({
            id: d.filename,
            filename: d.filename,
            chunks: d.chunks || 0,
            date: d.uploaded_at
              ? new Date(d.uploaded_at * 1000).toLocaleDateString()
              : '',
            status: d.status || 'uploaded',
            size: d.size_bytes,
          })));
        }
      } catch { /* backend not available */ }
      setLoadingDocs(false);
    })();
  }, []);

  const handleUpload = useCallback(async (info) => {
    const { file } = info;
    if (file.status === 'uploading') {
      setUploading(true);
      return;
    }
    setUploading(false);
    if (file.status === 'done') {
      const resp = file.response;
      setDocuments(prev => {
        // Replace if re-uploading same filename, else append
        const existing = prev.filter(d => d.filename !== (resp.filename || file.name));
        return [...existing, {
          id: resp.filename || file.name,
          filename: resp.filename || file.name,
          chunks: resp.chunks_created || 0,
          date: new Date().toLocaleDateString(),
          status: resp.status || 'uploaded',
        }];
      });
      message.success(`${file.name} uploaded — ${resp.chunks_created || 0} chunks indexed`);
    } else if (file.status === 'error') {
      message.error(`${file.name} upload failed`);
    }
  }, []);

  const handleDelete = async (doc) => {
    try {
      const resp = await fetch(
        `${API_BASE}/api/admin/uploaded-docs/${encodeURIComponent(doc.filename)}`,
        { method: 'DELETE' },
      );
      if (resp.ok) {
        setDocuments(prev => prev.filter(d => d.id !== doc.id));
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
          Upload data dictionaries, business rules, or table documentation
          to help RAVEN understand your data better.
        </p>

        <Upload.Dragger
          name="file"
          accept={ACCEPTED_TYPES}
          action={`${API_BASE}/api/admin/upload-doc`}
          onChange={handleUpload}
          showUploadList={false}
          disabled={uploading}
          className="upload-dragger"
        >
          <div className="upload-zone">
            <FileUp size={36} className="upload-icon" />
            <p className="upload-text">Drop files here or click to browse</p>
            <p className="upload-hint">Supported: .docx, .pdf, .md, .txt, .yaml — Max 10MB</p>
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
                  <span className="doc-item-name">{doc.filename}</span>
                  <span className="doc-item-meta">
                    {doc.chunks} chunks indexed · {doc.date}
                  </span>
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
            <p>No documents uploaded yet. Upload data dictionaries or business rules to improve query accuracy.</p>
          </div>
        )}
      </div>
    </div>
  );
}
