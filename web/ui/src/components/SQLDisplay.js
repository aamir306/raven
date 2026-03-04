import React, { useState } from 'react';

export default function SQLDisplay({ sql }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback
      const ta = document.createElement('textarea');
      ta.value = sql;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  // Basic SQL syntax highlighting
  const highlighted = highlightSQL(sql);

  return (
    <div className="section sql-container">
      <div className="section-header">
        <span>Generated SQL</span>
        <button className="copy-btn" onClick={handleCopy}>
          {copied ? 'Copied!' : 'Copy'}
        </button>
      </div>
      <div className="section-body">
        <pre dangerouslySetInnerHTML={{ __html: highlighted }} />
      </div>
    </div>
  );
}

function highlightSQL(sql) {
  const keywords = [
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
    'FULL', 'CROSS', 'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN',
    'LIKE', 'IS', 'NULL', 'AS', 'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT',
    'OFFSET', 'UNION', 'ALL', 'EXCEPT', 'INTERSECT', 'WITH', 'RECURSIVE',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'DISTINCT', 'TOP', 'INTO',
    'VALUES', 'SET', 'ASC', 'DESC', 'OVER', 'PARTITION', 'ROWS', 'RANGE',
    'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW', 'UNNEST',
    'TRY_CAST', 'CAST', 'COALESCE', 'NULLIF', 'FILTER',
  ];

  const functions = [
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ROUND', 'FLOOR', 'CEIL',
    'DATE_TRUNC', 'DATE_ADD', 'DATE_DIFF', 'CURRENT_DATE', 'CURRENT_TIMESTAMP',
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'FIRST_VALUE',
    'LAST_VALUE', 'NTH_VALUE', 'APPROX_DISTINCT', 'APPROX_PERCENTILE',
    'ARRAY_AGG', 'MAP_AGG', 'CONCAT', 'SUBSTR', 'LENGTH', 'LOWER', 'UPPER',
    'TRIM', 'REPLACE', 'REGEXP_LIKE', 'REGEXP_EXTRACT',
  ];

  let escaped = sql
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  // Highlight string literals
  escaped = escaped.replace(/'([^']*)'/g, "<span style='color:#a5d6a7'>'$1'</span>");

  // Highlight numbers
  escaped = escaped.replace(/\b(\d+\.?\d*)\b/g, "<span style='color:#f48fb1'>$1</span>");

  // Highlight keywords
  const kwPattern = new RegExp(`\\b(${keywords.join('|')})\\b`, 'gi');
  escaped = escaped.replace(kwPattern, (m) => `<span style='color:#82b1ff;font-weight:600'>${m.toUpperCase()}</span>`);

  // Highlight functions
  const fnPattern = new RegExp(`\\b(${functions.join('|')})\\s*\\(`, 'gi');
  escaped = escaped.replace(fnPattern, (m, fn) => `<span style='color:#c5e1a5;font-weight:600'>${fn.toUpperCase()}</span>(`);

  // Highlight comments
  escaped = escaped.replace(/--(.*?)$/gm, "<span style='color:#616161'>--$1</span>");

  return escaped;
}
