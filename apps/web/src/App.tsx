import { useEffect, useRef, useState } from 'react';
import { Link, useLocation, useNavigationType } from 'react-router';
import { loadDemo, type DemoData } from './data/demo';
import { Welcome } from './pages/Welcome';
import { DataPages } from './pages/DataPages';
import { EmptyState } from './components/ui';
import { navigation, SiteHeader } from './components/SiteHeader';
import { PwaActions, PwaNotice } from './pwa/PwaUi';
import { PersonalProvider } from './features/local/PersonalProvider';
import { fundLinkId, returnPoint } from './navigation/flow';

export function App() {
  return (
    <PersonalProvider>
      <AppContent />
    </PersonalProvider>
  );
}

function AppContent() {
  const location = useLocation();
  const navigationType = useNavigationType();
  const previousFocus = useRef<{ path: string; data?: DemoData } | undefined>(
    undefined,
  );
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
    document.title = `${location.pathname === '/' ? '看清每一份投资' : location.pathname.startsWith('/funds/') ? (data?.funds.find((fund) => `/funds/${fund.code}` === location.pathname)?.name ?? '基金详情') : (navigation.find(([path]) => path === location.pathname)?.[1] ?? '页面未找到')} · Sora`;
    const previous = previousFocus.current;
    previousFocus.current = { path: location.pathname, data };
    if (
      previous?.path === location.pathname &&
      previous.data === data &&
      navigationType !== 'POP'
    )
      return;
    const point =
      data && (navigationType === 'POP' || location.state?.restoreList)
        ? returnPoint(location.pathname + location.search)
        : undefined;
    const returnLink = point && document.getElementById(fundLinkId(point.code));
    if (returnLink && point) {
      returnLink.focus({ preventScroll: true });
      document.documentElement.scrollTop = point.y;
      document.body.scrollTop = point.y;
      const rect = returnLink.getBoundingClientRect();
      const headerHeight =
        document.querySelector('.site-header')?.getBoundingClientRect()
          .height ?? 0;
      if (rect.top < headerHeight + 12 || rect.bottom > window.innerHeight - 24)
        returnLink.scrollIntoView({ block: 'center' });
      return;
    }
    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;
    const editorInput = document.querySelector<HTMLElement>(
      '[data-position-autofocus]',
    );
    if (editorInput) {
      editorInput.focus();
      editorInput.scrollIntoView({ block: 'center' });
    } else {
      document.getElementById('main-content')?.focus({ preventScroll: true });
    }
  }, [
    location.key,
    data,
    location.pathname,
    location.search,
    location.state,
    navigationType,
  ]);
  const knownDataRoute = navigation.some(
    ([path]) =>
      path === location.pathname ||
      (path === '/funds' && /^\/funds\/[^/]+\/?$/.test(location.pathname)),
  );
  return (
    <>
      <a className="skip-link" href="#main-content">
        跳到主要内容
      </a>
      <div
        className={`app-shell ${location.pathname === '/' ? 'is-home' : ''}`}
      >
        <SiteHeader />
        <div className="workspace">
          <PwaNotice />
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
                  <span className="snapshot-label">示例快照</span> {data.asOf} ·
                  虚构示例，非真实行情
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
            <PwaActions />
          </footer>
        </div>
      </div>
    </>
  );
}
