import React, { useMemo, useCallback } from 'react';
import { Table, Input } from 'antd';
import { Download, FileSpreadsheet } from 'lucide-react';

export default function DataTab({ result }) {
  const { data } = result;

  const columns = useMemo(() => {
    if (!data?.length) return [];
    return Object.keys(data[0]).map(key => {
      const isNumeric = data.some(r => typeof r[key] === 'number');
      return {
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
        filterDropdown: ({ setSelectedKeys, selectedKeys, confirm, clearFilters }) => (
          <div style={{ padding: 8 }}>
            <Input
              placeholder={`Filter ${key}`}
              value={selectedKeys[0]}
              onChange={e => setSelectedKeys(e.target.value ? [e.target.value] : [])}
              onPressEnter={confirm}
              size="small"
              style={{ width: 180, marginBottom: 8, display: 'block' }}
            />
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="csv-btn" onClick={confirm} style={{ flex: 1 }}>Filter</button>
              <button className="csv-btn" onClick={clearFilters} style={{ flex: 1 }}>Reset</button>
            </div>
          </div>
        ),
        onFilter: (value, record) => {
          const cell = record[key];
          if (cell == null) return false;
          return String(cell).toLowerCase().includes(String(value).toLowerCase());
        },
        render: (val) => {
          if (val == null) return <span style={{ color: 'var(--text-muted)' }}>null</span>;
          if (typeof val === 'number') return val.toLocaleString();
          return String(val);
        },
        ellipsis: true,
        width: isNumeric ? 120 : undefined,
      };
    });
  }, [data]);

  const dataSource = useMemo(() => {
    if (!data) return [];
    return data.map((row, i) => ({ ...row, _key: i }));
  }, [data]);

  const downloadCSV = useCallback(() => {
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
    triggerDownload(blob, 'raven_results.csv');
  }, [data]);

  const downloadExcel = useCallback(() => {
    if (!data?.length) return;
    const cols = Object.keys(data[0]);
    /* Build a simple XLSX via XML spreadsheet (no library needed) */
    const escXml = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    let xml = '<?xml version="1.0"?><?mso-application progid="Excel.Sheet"?>';
    xml += '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" ';
    xml += 'xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">';
    xml += '<Worksheet ss:Name="Results"><Table>';
    /* Header row */
    xml += '<Row>' + cols.map(c => `<Cell><Data ss:Type="String">${escXml(c)}</Data></Cell>`).join('') + '</Row>';
    /* Data rows */
    for (const row of data) {
      xml += '<Row>';
      for (const c of cols) {
        const v = row[c];
        if (v == null) {
          xml += '<Cell><Data ss:Type="String"></Data></Cell>';
        } else if (typeof v === 'number') {
          xml += `<Cell><Data ss:Type="Number">${v}</Data></Cell>`;
        } else {
          xml += `<Cell><Data ss:Type="String">${escXml(v)}</Data></Cell>`;
        }
      }
      xml += '</Row>';
    }
    xml += '</Table></Worksheet></Workbook>';
    const blob = new Blob([xml], { type: 'application/vnd.ms-excel' });
    triggerDownload(blob, 'raven_results.xls');
  }, [data]);

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
        <div style={{ display: 'flex', gap: 6 }}>
          <button className="csv-btn" onClick={downloadCSV}>
            <Download size={12} /> CSV
          </button>
          <button className="csv-btn" onClick={downloadExcel}>
            <FileSpreadsheet size={12} /> Excel
          </button>
        </div>
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

function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
