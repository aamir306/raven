import React, { useMemo } from 'react';
import { Table } from 'antd';
import { Download } from 'lucide-react';

export default function DataTab({ result }) {
  const { data } = result;

  const columns = useMemo(() => {
    if (!data?.length) return [];
    return Object.keys(data[0]).map(key => ({
      title: key,
      dataIndex: key,
      key,
      sorter: (a, b) => {
        const va = a[key], vb = b[key];
        if (va == null) return 1;
        if (vb == null) return -1;
        if (typeof va === 'number' && typeof vb === 'number') return va - vb;
        return String(va).localeCompare(String(vb));
      },
      render: (val) => {
        if (val == null) return <span style={{ color: 'var(--text-muted)' }}>null</span>;
        if (typeof val === 'number') return val.toLocaleString();
        return String(val);
      },
      ellipsis: true,
    }));
  }, [data]);

  const dataSource = useMemo(() => {
    if (!data) return [];
    return data.map((row, i) => ({ ...row, _key: i }));
  }, [data]);

  const downloadCSV = () => {
    if (!data?.length) return;
    const cols = Object.keys(data[0]);
    const header = cols.join(',');
    const rows = data.map(row =>
      cols.map(c => {
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

  if (!data?.length) {
    return (
      <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
        Query returned no rows
      </div>
    );
  }

  return (
    <div className="data-tab">
      <div className="data-tab-header">
        <span className="row-count">{data.length.toLocaleString()} rows</span>
        <button className="csv-btn" onClick={downloadCSV}>
          <Download size={12} /> CSV
        </button>
      </div>

      <Table
        columns={columns}
        dataSource={dataSource}
        rowKey="_key"
        size="small"
        pagination={{
          pageSize: 25,
          showSizeChanger: true,
          pageSizeOptions: ['10', '25', '50', '100'],
          showTotal: (total) => `${total} rows`,
        }}
        scroll={{ x: 'max-content' }}
      />
    </div>
  );
}
