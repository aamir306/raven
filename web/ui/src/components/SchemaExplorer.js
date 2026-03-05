import React, { useMemo, useCallback } from 'react';
import ReactFlow, {
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
} from 'react-flow-renderer';
import { Database, Columns, X } from 'lucide-react';

/* ─── Table Node ─────────────────────────────────────────── */
function TableNode({ data }) {
  return (
    <div style={{
      background: 'var(--card-bg)',
      border: `2px solid ${data.isJoined ? '#6366f1' : 'var(--border)'}`,
      borderRadius: 10,
      minWidth: 220,
      maxWidth: 280,
      fontFamily: 'inherit',
      boxShadow: '0 2px 8px rgba(0,0,0,.15)',
    }}>
      {/* Header */}
      <div style={{
        background: data.isJoined ? '#6366f1' : 'var(--surface)',
        padding: '8px 12px',
        borderRadius: '8px 8px 0 0',
        display: 'flex', alignItems: 'center', gap: 6,
        borderBottom: '1px solid var(--border)',
      }}>
        <Database size={14} color={data.isJoined ? '#fff' : 'var(--text-muted)'} />
        <span style={{
          fontSize: 12, fontWeight: 600,
          color: data.isJoined ? '#fff' : 'var(--text)',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        }}>
          {data.label}
        </span>
        <span style={{
          fontSize: 10, marginLeft: 'auto',
          color: data.isJoined ? 'rgba(255,255,255,.7)' : 'var(--text-muted)',
        }}>
          {data.columns?.length || 0} cols
        </span>
      </div>

      {/* Columns */}
      <div style={{ padding: '6px 0', maxHeight: 200, overflowY: 'auto' }}>
        {(data.columns || []).slice(0, 15).map((col, i) => (
          <div key={i} style={{
            display: 'flex', alignItems: 'center', gap: 6,
            padding: '3px 12px', fontSize: 11,
            color: col.isJoinKey ? '#6366f1' : 'var(--text-secondary)',
          }}>
            <Columns size={10} style={{ flexShrink: 0 }} />
            <span style={{
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              fontWeight: col.isJoinKey ? 600 : 400,
            }}>
              {col.name}
            </span>
            <span style={{
              fontSize: 9, marginLeft: 'auto', flexShrink: 0,
              color: 'var(--text-muted)',
            }}>
              {col.type}
            </span>
          </div>
        ))}
        {(data.columns || []).length > 15 && (
          <div style={{ padding: '3px 12px', fontSize: 10, color: 'var(--text-muted)' }}>
            +{data.columns.length - 15} more…
          </div>
        )}
      </div>
    </div>
  );
}

const nodeTypes = { tableNode: TableNode };

/* ─── Build Graph From Debug Data ────────────────────────── */
function buildGraph(debug) {
  const tables = debug?.selected_tables || [];
  const relationships = debug?.relationships || [];
  const nodes = [];
  const edges = [];
  const tableSet = new Set(tables.map(t => (typeof t === 'string' ? t : t.table_name)));

  // Layout: grid
  const cols = Math.max(2, Math.ceil(Math.sqrt(tables.length)));
  tables.forEach((t, i) => {
    const tbl = typeof t === 'string' ? { table_name: t, columns: [] } : t;
    const row = Math.floor(i / cols);
    const col = i % cols;
    nodes.push({
      id: tbl.table_name,
      type: 'tableNode',
      position: { x: col * 300 + 50, y: row * 280 + 50 },
      data: {
        label: tbl.table_name,
        columns: (tbl.columns || []).map(c => ({
          name: typeof c === 'string' ? c : c.column_name || c.name,
          type: typeof c === 'string' ? '' : (c.data_type || c.type || ''),
          isJoinKey: false,
        })),
        isJoined: true,
      },
    });
  });

  // Edges from relationships
  relationships.forEach((rel, i) => {
    const src = rel.from_table || rel.source;
    const tgt = rel.to_table || rel.target;
    if (tableSet.has(src) && tableSet.has(tgt)) {
      edges.push({
        id: `e-${i}`,
        source: src,
        target: tgt,
        label: rel.join_key || rel.label || '',
        type: 'smoothstep',
        animated: true,
        style: { stroke: '#6366f1', strokeWidth: 2 },
        labelStyle: { fontSize: 10, fill: 'var(--text-muted)' },
      });

      // Mark join keys in source/target nodes
      const joinKey = rel.join_key || rel.on || '';
      if (joinKey) {
        [src, tgt].forEach(tName => {
          const node = nodes.find(n => n.id === tName);
          if (node) {
            node.data.columns.forEach(c => {
              if (joinKey.includes(c.name)) c.isJoinKey = true;
            });
          }
        });
      }
    }
  });

  return { nodes, edges };
}

/* ─── Schema Explorer Component ──────────────────────────── */
export default function SchemaExplorer({ debug, onClose }) {
  const { nodes: initNodes, edges: initEdges } = useMemo(
    () => buildGraph(debug), [debug]
  );

  const [nodes, , onNodesChange] = useNodesState(initNodes);
  const [edges, , onEdgesChange] = useEdgesState(initEdges);

  const onInit = useCallback((instance) => {
    setTimeout(() => instance.fitView({ padding: 0.2 }), 100);
  }, []);

  if (!debug?.selected_tables?.length) {
    return (
      <div className="schema-explorer-empty">
        <Database size={40} />
        <p>No schema data available for this query.</p>
        <button onClick={onClose} className="btn-secondary">Close</button>
      </div>
    );
  }

  return (
    <div className="schema-explorer-overlay">
      <div className="schema-explorer-panel">
        <div className="schema-explorer-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Database size={18} />
            <h3 style={{ margin: 0 }}>Schema Explorer</h3>
            <span className="badge badge-info">
              {nodes.length} tables
            </span>
          </div>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div style={{ flex: 1 }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
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
        </div>
      </div>
    </div>
  );
}
