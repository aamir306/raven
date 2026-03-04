import React, { useRef, useEffect } from 'react';

export default function QueryInput({ value, onChange, onSubmit, loading }) {
  const inputRef = useRef(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onSubmit();
    }
  };

  return (
    <div style={{
      display: 'flex',
      gap: '12px',
      alignItems: 'flex-end',
    }}>
      <textarea
        ref={inputRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Ask a question about your data..."
        disabled={loading}
        rows={2}
        style={{
          flex: 1,
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: '8px',
          padding: '12px 16px',
          color: 'var(--text)',
          fontSize: '1rem',
          fontFamily: 'inherit',
          resize: 'vertical',
          minHeight: '52px',
          outline: 'none',
        }}
      />
      <button
        onClick={onSubmit}
        disabled={loading || !value.trim()}
        style={{
          background: 'var(--accent)',
          color: '#fff',
          border: 'none',
          borderRadius: '8px',
          padding: '12px 24px',
          fontSize: '0.9rem',
          fontWeight: 600,
          cursor: loading || !value.trim() ? 'not-allowed' : 'pointer',
          opacity: loading || !value.trim() ? 0.5 : 1,
          whiteSpace: 'nowrap',
          height: '52px',
        }}
      >
        {loading ? 'Working...' : 'Ask RAVEN'}
      </button>
    </div>
  );
}
