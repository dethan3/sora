import { useEffect, useState } from 'react';
import { Link, NavLink, useLocation } from 'react-router';
import { loadDemo, type DemoData } from './data/demo';
import { Welcome } from './pages/Welcome';
import { DataPages } from './pages/DataPages';
import { EmptyState } from './components/ui';

const navigation = [
  ['/', 'Welcome'],
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
    document.title = `${navigation.find(([path]) => path === location.pathname)?.[1] ?? '页面未找到'} · Sora`;
    document.getElementById('main-content')?.focus();
  }, [location.pathname]);
  const knownDataRoute = navigation
    .slice(1)
    .some(([path]) => path === location.pathname);
  return (
    <>
      <a className="skip-link" href="#main-content">
        跳到主要内容
      </a>
      <div className="app-shell">
        <aside className="sidebar">
          <Link to="/" className="brand" aria-label="Sora 首页">
            sora<span>○</span>
          </Link>
          <p className="brand-caption">投资，清晰一点。</p>
          <nav aria-label="主导航">
            {navigation.map(([path, label], i) => (
              <NavLink end to={path} key={path}>
                <span aria-hidden="true" className="nav-index">
                  0{i}
                </span>
                {label}
              </NavLink>
            ))}
          </nav>
          <div className="sidebar-foot">
            <span className="status-dot" /> M0 · Foundation
            <p>从容观察，长期思考。</p>
          </div>
        </aside>
        <div className="workspace">
          <div className="topbar">
            <span>个人投资观察空间</span>
            <span className="demo-badge">仅示例数据</span>
          </div>
          <main id="main-content" tabIndex={-1}>
            {location.pathname === '/' ? (
              <Welcome />
            ) : !knownDataRoute ? (
              <EmptyState title="没有找到这个页面">
                <Link to="/">返回 Welcome</Link>
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
                  示例快照 · {data.asOf} · 虚构数据，非投资建议
                </p>
                <DataPages data={data} />
              </>
            ) : (
              <p role="status">正在读取示例数据…</p>
            )}
          </main>
          <footer>
            Sora V2 <span>一个更清晰的开始 · M0 Preview</span>
          </footer>
        </div>
      </div>
    </>
  );
}
