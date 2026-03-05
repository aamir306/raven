import React, { useState, useEffect, useMemo, useCallback } from 'react';
import ReactFlow, {
  MiniMap, Controls, Background,
  useNodesState, useEdgesState,
} from 'react-flow-renderer';
import { Database, Columns, X, Search, Filter } from 'lucide-react';

const API_BASE = process.env.REACT_APP_API_URL || '';

/* ─── Table Node ─────────────────────────────────────────── */
function TableNode({ data }) {
  return (
    <div style={{
      background: 'var(--surface)',
      border: `2px solid ${data.highlight ? '#6366f1' : 'var(--border)'}`,
      borderRadius: 10, minWidth: 220, maxWidth: 280,
      fontFamily: 'inherit',
      boxShadow: '0 2px 8px rgba(0,0,0,.15)',
    }}>
      <div style={{
        background: data.highlight ? '#6366f1' : 'var(--surface-hover)',
        padding: '8px 12px', borderRadius: '8px 8px 0 0',
        display: 'flex', alignItems: 'center', gap: 6,
        borderBottom: '1px solid var(--border)',
      }}>
        <Database size={14} color={data.highlight ? '#fff' : 'var(--text-muted)'} />
        <span style={{
          fontSize: 12, fontWeight: 600,
          color: data.highlight ? '#fff' : 'var(--text)',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>
          {data.label}
        </span>
        <span style={{
          fontSize: 10, marginLeft: 'auto',
          color: data.highlight ? 'rgba(255,255,255,.7)' : 'var(--text-muted)',
        }}>
          {data.columns?.length || 0} cols
        </span>
      </div>
      <div style={{ padding: '6px 0', maxHeight: 200, overflowY: 'auto' }}>
        {(data.columns || []).slice(0, 12).map((col, i) => (
          <div key={i} style={{
            display: 'flex', alignItems: 'center', gap: 6,
            padding: '3px 12px', fontSize: 11,
            color: col.isKey ? '#6366f1' : 'var(--text-dim)',
          }}>
            <Columns size={10} style={{ flexShrink: 0 }} />
            <span style={{
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              fontWeight: col.isKey ? 600 : 400,
            }}>
              {col.name}
            </span>
            <span style={{ fontSize: 9, marginLeft: 'auto', flexShrink: 0, color: 'var(--text-muted)' }}>
              {col.type}
            </span>
          </div>
        ))}
        {(data.columns || []).length > 12 && (
          <div style={{ padding: '3px 12px', fontSize: 10, color: 'var(--text-muted)' }}>
            +{data.columns.length - 12} more…
          </div>
        )}
      </div>
    </div>
  );
}

const nodeTypes = { tableNode: TableNode };

/* ─── Demo schema data (fallback when API unavailable) ─── */
const DEMO_TABLES = [
  { table_name: 'gold.daily_active_users', columns: [
    { name: 'ds', type: 'DATE' }, { name: 'user_id', type: 'VARCHAR' },
    { name: 'segment', type: 'VARCHAR' }, { name: 'platform', type: 'VARCHAR' },
    { name: 'is_premium', type: 'BOOLEAN' },
  ]},
  { table_name: 'gold.orders', columns: [
    { name: 'order_id', type: 'VARCHAR' }, { name: 'user_id', type: 'VARCHAR' },
    { name: 'order_date', type: 'DATE' }, { name: 'amount', type: 'DOUBLE' },
    { name: 'status', type: 'VARCHAR' }, { name: 'batch_id', type: 'VARCHAR' },
  ]},
  { table_name: 'gold.revenue_daily', columns: [
    { name: 'ds', type: 'DATE' }, { name: 'revenue', type: 'DOUBLE' },
    { name: 'channel', type: 'VARCHAR' }, { name: 'product', type: 'VARCHAR' },
  ]},
  { table_name: 'gold.batches', columns: [
    { name: 'batch_id', type: 'VARCHAR' }, { name: 'batch_name', type: 'VARCHAR' },
    { name: 'start_date', type: 'DATE' }, { name: 'end_date', type: 'DATE' },
    { name: 'status', type: 'VARCHAR' }, { name: 'category', type: 'VARCHAR' },
  ]},
  { table_name: 'silver.user_sessions', columns: [
    { name: 'ds', type: 'DATE' }, { name: 'user_id', type: 'VARCHAR' },
    { name: 'session_count', type: 'BIGINT' }, { name: 'total_minutes', type: 'DOUBLE' },
  ]},
];

const DEMO_RELATIONSHIPS = [
  { from_table: 'gold.daily_active_users', to_table: 'gold.orders', join_key: 'user_id' },
  { from_table: 'gold.orders', to_table: 'gold.batches', join_key: 'batch_id' },
  { from_table: 'gold.daily_active_users', to_table: 'silver.user_sessions', join_key: 'user_id' },
];

/* ─── Build Graph ─── */
function buildGraph(tables, relationships, searchTerm) {
  const nodes = [];
  const edges = [];
  const lowerSearch = searchTerm.toLowerCase();
  const filtered = searchTerm
    ? tables.filter(t => t.table_name.toLowerCase().includes(lowerSearch))
    : tables;
  const tableSet = new Set(filtered.map(t => t.table_name));
  const cols = Math.max(2, Math.ceil(Math.sqrt(filtered.length)));

  filtered.forEach((t, i) => {
    const row = Math.floor(i / cols);
    const col = i % cols;
    nodes.push({
      id: t.table_name,
      type: 'tableNode',
      position: { x: col * 310 + 50, y: row * 280 + 50 },
      data: {
        label: t.table_name,
        columns: (t.columns || []).map(c => ({
          name: c.name || c.column_name, type: c.type || c.data_type || '',
          isKey: (c.name || '').endsWith('_id') || c.name === 'ds',
        })),
        highlight: searchTerm ? true : false,
      },
    });
  });

  (relationships || []).forEach((rel, i) => {
    const src = rel.from_table || rel.source;
    const tgt = rel.to_table || rel.target;
    if (tableSet.has(src) && tableSet.has(tgt)) {
      edges.push({
        id: `e-${i}`, source: src, target: tgt,
        label: rel.join_key || '', type: 'smoothstep', animated: true,
        style: { stroke: '#6366f1', strokeWidth: 2 },
        labelStyle: { fontSize: 10, fill: 'var(--text-muted)' },
      });
    }
  });

  return { nodes, edges };
}

/* ─── Schema Explorer Page ─── */
export default function SchemaExplorerPage({ onClose }) {
  const [tables, setTables] = useState([]);
  const [relationships, setRelationships] = useState([]);
  const [search, setSearch] = useState('');
  const [tierFilter, setTierFilter] = useState('all');
  const [loadingSchema, setLoadingSchema] = useState(true);

  useEffect(() => {
    setLoadingSchema(true);
    fetch(`${API_BASE}/api/schema/tables`)
      .then(r => r.json())
      .then(data => {
        setTables(data.tables || []);
        setRelationships(data.relationships || []);
      })
      .catch(() => {
        setTables(DEMO_TABLES);
        setRelationships(DEMO_RELATIONSHIPS);
      })
      .finally(() => setLoadingSchema(false));
  }, []);

  const filteredTables = useMemo(() => {
    let list = tables;
    if (tierFilter !== 'all') {
      list = list.filter(t => t.table_name.startsWith(tierFilter + '.'));
    }
    return list;
  }, [tables, tierFilter]);

  const { nodes: initNodes, edges: initEdges } = useMemo(
    () => buildGraph(filteredTables, relationships, search),
    [filteredTables, relationships, search]
  );

  const [nodes, , onNodesChange] = useNodesState(initNodes);
  const [edges, , onEdgesChange] = useEdgesState(initEdges);

  // Re-set nodes when search/filter changes
  useEffect(() => {
    const { nodes: n, edges: e } = buildGraph(filteredTables, relationships, search);
    onNodesChange(n.map(node => ({ type: 'reset', item: node })));
    onEdgesChange(e.map(edge => ({ type: 'reset', item: edge })));
  }, [filteredTables, relationships, search, onNodesChange, onEdgesChange]);

  const onInit = useCallback((instance) => {
    setTimeout(() => instance.fitView({ padding: 0.2 }), 200);
  }, []);

  const tiers = useMemo(() => {
    const s = new Set(tables.map(t => t.table_name.split('.')[0]));
    return ['all', ...Array.from(s).sort()];
  }, [tables]);

  return (
    <div className="page-panel" style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div className="page-panel-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, flex: 1 }}>
          <Database size={18} />
          <h2 style={{ margin: 0, fontSize: '1rem' }}>Schema Explorer</h2>
          <span className="badge badge-info" style={{ fontSize: 11 }}>
            {filteredTables.length} tables
          </span>

          <div style={{ marginLeft: 16, display: 'flex', alignItems: 'center', gap: 8 }}>
            <div className="glossary-search" style={{ width: 220 }}>
              <Search size={13} />
              <input
                placeholder="Search tables..."
                value={search}
                onChange={e => setSearch(e.target.value)}
              />
            </div>
            <div style={{ display: 'flex', gap: 4 }}>
              {tiers.map(t => (
                <button
                  key={t}
                  className={`chart-type-btn ${tierFilter === t ? 'active' : ''}`}
                  onClick={() => setTierFilter(t)}
                  style={{ fontSize: 11, padding: '2px 8px' }}
                >
                  {t === 'all' ? 'All' : t}
                </button>
              ))}
            </div>
          </div>
        </div>
        <button className="btn-icon-sm" onClick={onClose}><X size={16} /></button>
      </div>

      <div style={{ flex: 1, minHeight: 400 }}>
        {loadingSchema ? (
          <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)' }}>
            Loading schema...
          </div>
        ) : initNodes.length === 0 ? (
          <div className="empty-state">
            <Filter size={32} />
            <p>No tables match your search.</p>
          </div>
        ) : (
          <ReactFlow
            nodes={initNodes}
            edges={initEdges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onInit={onInit}
            nodeTypes={nodeTypes}
            fitView
            attributionPosition="bottom-left"
          >
            <Controls />
            <MiniMap
              nodeColor="#6366f1"
              maskColor="rgba(0,0,0,.2)"
              style={{ background: 'var(--surface)' }}
            />
            <Background color="var(--border)" gap={16} />
          </ReactFlow>
        )}
      </div>
    </div>
  );
}
