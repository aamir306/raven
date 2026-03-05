import React, { useState, useMemo, useRef, useCallback } from 'react';
import { Download } from 'lucide-react';
import createPlotlyComponent from 'react-plotly.js/factory';
import Plotly from 'plotly.js-dist-min';

const Plot = createPlotlyComponent(Plotly);

const CHART_TYPES = ['bar', 'line', 'scatter', 'pie'];

export default function ChartTab({ result, theme }) {
  const { data, chart_config, chart_type } = result;
  const [selectedType, setSelectedType] = useState(
    chart_type?.toLowerCase() === 'kpi' ? 'bar' : (chart_type?.toLowerCase() || 'bar')
  );
  const plotRef = useRef(null);

  // Auto-detect columns for charting
  const chartData = useMemo(() => {
    if (!data || data.length === 0) return null;
    const cols = Object.keys(data[0]);
    if (cols.length < 2) return null;

    // First string column = x-axis, first numeric column = y-axis
    let xCol = cols[0];
    let yCols = [];
    for (const c of cols) {
      const sample = data.find(r => r[c] != null)?.[c];
      if (typeof sample === 'number') {
        yCols.push(c);
      } else if (yCols.length === 0) {
        xCol = c;
      }
    }
    if (yCols.length === 0) yCols = [cols[1]];
    return { xCol, yCols };
  }, [data]);

  const plotLayout = useMemo(() => ({
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: { color: theme === 'dark' ? '#94a3b8' : '#475569', size: 12 },
    margin: { t: 30, r: 20, b: 50, l: 60 },
    xaxis: {
      gridcolor: theme === 'dark' ? '#334155' : '#e2e8f0',
      zerolinecolor: theme === 'dark' ? '#334155' : '#e2e8f0',
    },
    yaxis: {
      gridcolor: theme === 'dark' ? '#334155' : '#e2e8f0',
      zerolinecolor: theme === 'dark' ? '#334155' : '#e2e8f0',
    },
    legend: { orientation: 'h', y: -0.2 },
    autosize: true,
  }), [theme]);

  const plotTraces = useMemo(() => {
    if (!chartData || !data) return [];
    const colors = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#06b6d4', '#ec4899'];

    if (selectedType === 'pie') {
      return [{
        type: 'pie',
        labels: data.map(r => String(r[chartData.xCol] ?? '')),
        values: data.map(r => Number(r[chartData.yCols[0]] ?? 0)),
        marker: { colors },
        textinfo: 'label+percent',
        hole: 0.4,
      }];
    }

    return chartData.yCols.map((col, i) => ({
      type: selectedType === 'line' ? 'scatter' : selectedType,
      mode: selectedType === 'scatter' ? 'markers' : (selectedType === 'line' ? 'lines+markers' : undefined),
      x: data.map(r => r[chartData.xCol]),
      y: data.map(r => Number(r[col] ?? 0)),
      name: col,
      marker: { color: colors[i % colors.length], size: selectedType === 'scatter' ? 8 : undefined },
      line: selectedType === 'line' ? { width: 2, shape: 'spline' } : undefined,
    }));
  }, [data, chartData, selectedType]);

  const handleDownload = useCallback(() => {
    if (!plotRef.current?.el) return;
    Plotly.downloadImage(plotRef.current.el, {
      format: 'png', width: 1200, height: 600, filename: 'raven_chart',
    });
  }, []);

  const handleDownloadSVG = useCallback(() => {
    if (!plotRef.current?.el) return;
    Plotly.downloadImage(plotRef.current.el, {
      format: 'svg', width: 1200, height: 600, filename: 'raven_chart',
    });
  }, []);

  if (!data || data.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
        No data to chart
      </div>
    );
  }

  return (
    <div>
      <div className="chart-controls">
        {CHART_TYPES.map(t => (
          <button
            key={t}
            className={`chart-type-btn ${selectedType === t ? 'active' : ''}`}
            onClick={() => setSelectedType(t)}
          >
            {t.charAt(0).toUpperCase() + t.slice(1)}
          </button>
        ))}
        <button className="chart-type-btn" onClick={handleDownload} style={{ marginLeft: 'auto' }}>
          <Download size={12} /> PNG
        </button>
        <button className="chart-type-btn" onClick={handleDownloadSVG}>
          <Download size={12} /> SVG
        </button>
      </div>

      <div className="chart-wrapper">
        <Plot
          ref={plotRef}
          data={plotTraces}
          layout={plotLayout}
          config={{
            responsive: true,
            displayModeBar: false,
          }}
          style={{ width: '100%', height: 350 }}
        />
      </div>
    </div>
  );
}
