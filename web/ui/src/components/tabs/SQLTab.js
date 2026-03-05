import React, { useState, useCallback } from 'react';
import Editor from '@monaco-editor/react';
import { Copy, Check, Play } from 'lucide-react';

export default function SQLTab({ result, theme, onRerun }) {
  const [copied, setCopied] = useState(false);
  const [editedSQL, setEditedSQL] = useState(null);
  const [isEditing, setIsEditing] = useState(false);

  const sql = editedSQL ?? result.sql;

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(sql);
    } catch {
      const ta = document.createElement('textarea');
      ta.value = sql;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [sql]);

  const handleEditRerun = () => {
    if (isEditing && editedSQL && onRerun) {
      // Re-run with the edited SQL
      onRerun(editedSQL);
      setIsEditing(false);
      return;
    }
    setIsEditing(!isEditing);
    if (!isEditing) {
      setEditedSQL(result.sql);
    }
  };

  return (
    <div>
      <div className="sql-tab-actions">
        <button className="sql-action-btn" onClick={handleCopy}>
          {copied ? <><Check size={13} /> Copied!</> : <><Copy size={13} /> Copy SQL</>}
        </button>
        <button
          className={`sql-action-btn ${isEditing ? 'primary' : ''}`}
          onClick={handleEditRerun}
        >
          <Play size={13} /> {isEditing ? 'Run Edited SQL' : 'Edit & Re-run'}
        </button>
      </div>

      <div className="monaco-wrapper">
        <Editor
          height={Math.min(Math.max(sql.split('\n').length * 19, 100), 400)}
          language="sql"
          value={sql}
          onChange={(v) => isEditing && setEditedSQL(v)}
          theme={theme === 'dark' ? 'vs-dark' : 'light'}
          options={{
            readOnly: !isEditing,
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            fontSize: 13,
            fontFamily: "'SF Mono', 'Fira Code', 'JetBrains Mono', 'Consolas', monospace",
            lineNumbers: 'on',
            renderLineHighlight: 'none',
            padding: { top: 12, bottom: 12 },
            automaticLayout: true,
            wordWrap: 'on',
            folding: true,
            contextmenu: false,
            suggest: { enabled: isEditing },
          }}
        />
      </div>
    </div>
  );
}
