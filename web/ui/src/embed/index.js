/**
 * @raven-sql/embed — Standalone entrypoint
 *
 * Usage in any HTML page:
 *   <div id="raven-widget"></div>
 *   <script src="raven-widget.js"></script>
 *   <script>
 *     RavenEmbed.init({
 *       container: '#raven-widget',
 *       apiUrl: 'https://your-api.com/api/query',
 *       token: 'optional-auth-token',
 *     });
 *   </script>
 *
 * Usage in React:
 *   import { RavenWidget } from '@raven-sql/embed';
 *   <RavenWidget apiUrl="..." token="..." />
 */

export { default as RavenWidget } from './RavenWidget';

// Vanilla JS initializer for non-React apps
export function init(options = {}) {
  const {
    container = '#raven-widget',
    apiUrl = '/api/query',
    token = null,
    title = 'Ask RAVEN',
    subtitle = 'AI-powered SQL assistant',
    suggestions = [],
    contextTables = [],
  } = options;

  const el = typeof container === 'string'
    ? document.querySelector(container)
    : container;

  if (!el) {
    console.error('[RavenEmbed] Container not found:', container);
    return;
  }

  // Lazy-load React for vanilla JS usage
  Promise.all([
    import('react'),
    import('react-dom/client'),
    import('./RavenWidget'),
  ]).then(([React, ReactDOMClient, { default: RavenWidget }]) => {
    const root = ReactDOMClient.createRoot(el);
    root.render(
      React.createElement(RavenWidget, {
        apiUrl, token, title, subtitle, suggestions, contextTables,
      })
    );
  });
}

// Expose globally for script tag usage
if (typeof window !== 'undefined') {
  window.RavenEmbed = { init };
}
