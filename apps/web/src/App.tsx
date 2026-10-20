import { useEffect, useState } from 'react';
import { Link, NavLink, useLocation } from 'react-router';
import { loadDemo, type DemoData } from './data/demo';
import { Welcome } from './pages/Welcome';
import { DataPages } from './pages/DataPages';
import { EmptyState } from './components/ui';

const navigation = [
  ['/discover', '发现'],
  ['/funds', '基金'],
  ['/watchlist', '关注'],
  ['/portfolio', '我的组合'],
] as const;

export function App() {
  const location = useLocation();
  const [data, setData] = useState<DemoData>();
  const [error, setError] = useState(false);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setError(false);
    loadDemo(controller.signal)
      .then((result) => {
        if (!controller.signal.aborted) setData(result);
      })
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      });
    return () => controller.abort();
  }, [attempt]);
  useEffect(() => {
    document.title = `${location.pathname === '/' ? '看清每一份投资' : (navigation.find(([path]) => path === location.pathname)?.[1] ?? '页面未找到')} · Sora`;
    document.getElementById('main-content')?.focus({ preventScroll: true });
    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;
  }, [location.pathname]);
  const knownDataRoute = navigation.some(
    ([path]) => path === location.pathname,
  );
  return (
    <>
      <a className="skip-link" href="#main-content">
        跳到主要内容
      </a>
      <div
        className={`app-shell ${location.pathname === '/' ? 'is-home' : ''}`}
      >
        <header className="site-header">
          <div className="header-inner">
            <Link
              to="/"
              className="brand"
              aria-label="Sora 首页"
              aria-current={location.pathname === '/' ? 'page' : undefined}
            >
              <span className="brand-mark" aria-hidden="true">
                <i />
                <i />
                <i />
              </span>
              sora
            </Link>
            <nav aria-label="主导航">
              {navigation.map(([path, label]) => (
                <NavLink end to={path} key={path}>
                  {label}
                </NavLink>
              ))}
            </nav>
            <span className="demo-badge">
              <span className="status-dot" />
              示例预览
            </span>
          </div>
        </header>
        <div className="workspace">
          <main id="main-content" tabIndex={-1}>
            {location.pathname === '/' ? (
              <Welcome data={data} />
            ) : !knownDataRoute ? (
              <EmptyState title="没有找到这个页面">
                <Link to="/">返回首页</Link>
              </EmptyState>
            ) : error ? (
              <div role="alert">
                <EmptyState title="暂时无法读取示例">
                  请检查连接后重试。
                </EmptyState>
                <button
                  className="action"
                  onClick={() => setAttempt((value) => value + 1)}
                >
                  重新加载
                </button>
              </div>
            ) : data ? (
              <>
                <p className="data-date">
                  <span className="snapshot-label">DEMO SNAPSHOT</span>{' '}
                  {data.asOf} · 虚构示例，非真实行情
                </p>
                <DataPages data={data} />
              </>
            ) : (
              <p role="status">正在读取示例数据…</p>
            )}
          </main>
          <footer>
            <span className="footer-brand">
              sora<span>投资，清晰可见。</span>
            </span>
            <span>示例数据仅供演示 · 非投资建议</span>
          </footer>
        </div>
      </div>
    </>
  );
}
