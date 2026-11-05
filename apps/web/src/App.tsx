import { Routes, Route } from 'react-router';
import { InstrumentPage } from './features/ingest/InstrumentPage';
import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { Link, useLocation, useNavigationType } from 'react-router';
import { loadCatalog, type CatalogResult, type DemoData } from './data/demo';
import { Welcome } from './pages/Welcome';
import { DataPages } from './pages/DataPages';
import { EmptyState } from './components/ui';
import { navigation, SiteHeader } from './components/SiteHeader';
import { PwaActions, PwaNotice } from './pwa/PwaUi';
import { PersonalProvider } from './features/local/PersonalProvider';
import { fundLinkId, returnPoint } from './navigation/flow';
import { DirectionProvider } from './features/discover/DirectionProvider';
import { research } from './features/research/model';
import { AuthProvider } from './features/auth/AuthProvider';
import { AuthPage } from './features/auth/AuthPage';
import { AccountPage } from './features/auth/AccountPage';
import { AdminPage } from './features/admin/AdminPage';

export function App() {
  return (
    <AuthProvider>
      <DirectionProvider>
        <AppContent />
      </DirectionProvider>
    </AuthProvider>
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
  const [catalogOrigin, setCatalogOrigin] = useState<CatalogResult['origin']>();
  const [loading, setLoading] = useState(true);
  const authMode =
    location.pathname === '/login'
      ? 'login'
      : location.pathname === '/register'
        ? 'register'
        : undefined;
  useEffect(() => {
    const controller = new AbortController();
    setError(false);
    setLoading(true);
    loadCatalog(controller.signal)
      .then((result) => {
        if (!controller.signal.aborted) {
          setData(result.data);
          setCatalogOrigin(result.origin);
        }
      })
      .catch(() => {
        if (!controller.signal.aborted) setError(true);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [attempt]);
  useEffect(() => {
    const changed = () => setAttempt((value) => value + 1);
    window.addEventListener('sora-catalog-changed', changed);
    return () => window.removeEventListener('sora-catalog-changed', changed);
  }, []);
  useLayoutEffect(() => {
    document.title = `${location.pathname === '/' ? '看清每一份投资' : location.pathname === '/research' ? '研究线索' : location.pathname === '/following' ? '我的关注' : location.pathname.startsWith('/research/') ? (research.find((signal) => '/research/' + signal.id === location.pathname)?.title ?? '研究线索') : location.pathname.startsWith('/discover/') ? (data?.modules.find((item) => '/discover/' + item.id === location.pathname)?.title ?? '方向详情') : location.pathname.startsWith('/funds/') ? (data?.funds.find((fund) => `/funds/${fund.code}` === location.pathname)?.name ?? '基金详情') : (navigation.find(([path]) => path === location.pathname)?.[1] ?? '页面未找到')} · Sora`;
    if (authMode || location.pathname === '/account')
      document.title = `${authMode === 'login' ? '登录' : authMode === 'register' ? '邀请码注册' : '我的账号'} · Sora`;
    if (location.pathname === '/admin') document.title = '管理后台 · Sora';
    if (location.pathname.startsWith('/instruments/'))
      document.title = '证券详情 · Sora';
    const previous = previousFocus.current;
    previousFocus.current = { path: location.pathname, data };
    if (/^#[a-z0-9-]+$/.test(location.hash)) {
      const target = document.getElementById(location.hash.slice(1));
      if (target) {
        target.scrollIntoView({ block: 'start' });
        if (target.id === 'main-content') target.focus({ preventScroll: true });
        return;
      }
    }
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
    location.hash,
    location.state,
    navigationType,
  ]);
  const knownDataRoute =
    location.pathname === '/research' ||
    location.pathname === '/following' ||
    /^\/research\/[a-z0-9-]+\/?$/.test(location.pathname) ||
    navigation.some(
      ([path]) =>
        path === location.pathname ||
        (path === '/funds' && /^\/funds\/[^/]+\/?$/.test(location.pathname)) ||
        (path === '/discover' &&
          /^\/discover\/[a-z0-9-]+\/?$/.test(location.pathname)),
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
          {catalogOrigin === 'fallback' && data && (
            <div className="pwa-notice catalog-notice">
              <span role="status">
                在线目录暂不可用，正在使用随应用保存的公共资料（{data.asOf}
                ），可能不含最新收录的标的。
              </span>
              <button
                className="button-quiet"
                disabled={loading}
                onClick={() => setAttempt((value) => value + 1)}
              >
                {loading ? '正在重试…' : '重试在线目录'}
              </button>
            </div>
          )}
          <main id="main-content" tabIndex={-1}>
            {authMode ? (
              <AuthPage key={authMode} mode={authMode} />
            ) : location.pathname === '/account' ? (
              <PersonalProvider>
                <AccountPage />
              </PersonalProvider>
            ) : location.pathname === '/admin' ? (
              <AdminPage />
            ) : location.pathname.startsWith('/instruments/') ? (
              <Routes>
                <Route path="/instruments/:id" element={<InstrumentPage />} />
              </Routes>
            ) : location.pathname === '/' ? (
              <PersonalProvider>
                <Welcome data={data} />
              </PersonalProvider>
            ) : !knownDataRoute ? (
              <EmptyState title="没有找到这个页面">
                <Link to="/">返回首页</Link>
              </EmptyState>
            ) : error ? (
              <div role="alert">
                <EmptyState title="暂时无法读取数据">
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
              <PersonalProvider>
                <DataPages data={data} />
              </PersonalProvider>
            ) : (
              <div role="status" aria-label="正在载入内容">
                <p>正在载入内容…</p>
                <div className="direction-skeleton" aria-hidden="true">
                  <div className="direction-loading" />
                  <div className="direction-loading" />
                  <div className="direction-loading" />
                </div>
              </div>
            )}
          </main>
          <footer>
            <span className="footer-brand">
              sora<span>投资，清晰可见。</span>
            </span>
            <span>独立观察，理性判断。</span>
            <PwaActions />
          </footer>
        </div>
      </div>
    </>
  );
}
