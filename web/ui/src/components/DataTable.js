import React, { useState, useMemo } from 'react';

const PAGE_SIZE = 25;

export default function DataTable({ data }) {
  const [page, setPage] = useState(0);
  const [sortCol, setSortCol] = useState(null);
  const [sortAsc, setSortAsc] = useState(true);

  const columns = useMemo(() => {
    if (!data?.length) return [];
    return Object.keys(data[0]);
  }, [data]);

  const sorted = useMemo(() => {
    if (!sortCol) return data;
    return [...data].sort((a, b) => {
      const va = a[sortCol];
      const vb = b[sortCol];
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === 'number' && typeof vb === 'number') {
        return sortAsc ? va - vb : vb - va;
      }
      return sortAsc
        ? String(va).localeCompare(String(vb))
        : String(vb).localeCompare(String(va));
    });
  }, [data, sortCol, sortAsc]);

  const pageData = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const totalPages = Math.ceil(data.length / PAGE_SIZE);

  const handleSort = (col) => {
    if (sortCol === col) {
      setSortAsc(!sortAsc);
    } else {
      setSortCol(col);
      setSortAsc(true);
    }
  };

  const downloadCSV = () => {
    const header = columns.join(',');
    const rows = data.map(row =>
      columns.map(c => {
        const v = row[c];
        if (v == null) return '';
        const s = String(v);
        return s.includes(',') || s.includes('"') || s.includes('\n')
          ? `"${s.replace(/"/g, '""')}"`
          : s;
      }).join(',')
    );
    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'raven_results.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  if (!data?.length) return null;

  return (
    <div className="section">
      <div className="section-header">
        <span>Results ({data.length} rows)</span>
        <button className="download-btn" onClick={downloadCSV}>Download CSV</button>
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table className="data-table">
          <thead>
            <tr>
              {columns.map(col => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  style={{ cursor: 'pointer', userSelect: 'none' }}
                >
                  {col}
                  {sortCol === col && (sortAsc ? ' ▲' : ' ▼')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageData.map((row, i) => (
              <tr key={i}>
                {columns.map(col => (
                  <td key={col}>{formatValue(row[col])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="table-footer">
          <span>Page {page + 1} of {totalPages}</span>
          <div style={{ display: 'flex', gap: '8px' }}>
            <button
              className="copy-btn"
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0}
            >
              Prev
            </button>
            <button
              className="copy-btn"
              onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function formatValue(v) {
  if (v == null) return <span style={{ color: 'var(--text-dim)' }}>null</span>;
  if (typeof v === 'number') {
    return Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return String(v);
}
