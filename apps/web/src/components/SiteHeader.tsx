import { useEffect, useRef, useState } from 'react';
import { Link, NavLink, useLocation } from 'react-router';

export const navigation = [
  ['/discover', '发现'],
  ['/funds', '基金'],
  ['/watchlist', '关注'],
  ['/portfolio', '我的组合'],
] as const;

export function SiteHeader() {
  const location = useLocation();
  const [open, setOpen] = useState(false);
  const header = useRef<HTMLElement>(null);
  const toggle = useRef<HTMLButtonElement>(null);

  useEffect(() => setOpen(false), [location.pathname, location.search]);
  useEffect(() => {
    const desktop = window.matchMedia?.('(min-width: 768px)');
    const reset = () => {
      if (desktop?.matches) setOpen(false);
    };
    desktop?.addEventListener('change', reset);
    return () => desktop?.removeEventListener('change', reset);
  }, []);
  useEffect(() => {
    if (!open) return;
    const dismiss = (event: PointerEvent) => {
      if (!header.current?.contains(event.target as Node)) setOpen(false);
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      setOpen(false);
      toggle.current?.focus();
    };
    document.addEventListener('pointerdown', dismiss);
    document.addEventListener('keydown', escape);
    return () => {
      document.removeEventListener('pointerdown', dismiss);
      document.removeEventListener('keydown', escape);
    };
  }, [open]);

  function navigate(path: string) {
    setOpen(false);
    if (path === location.pathname) {
      document.getElementById('main-content')?.focus({ preventScroll: true });
    }
  }

  return (
    <header
      className="site-header"
      ref={header}
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) setOpen(false);
      }}
    >
      <div className="header-inner">
        <Link
          to="/"
          className="brand"
          aria-label="Sora 首页"
          aria-current={location.pathname === '/' ? 'page' : undefined}
          onClick={() => navigate('/')}
        >
          <span className="brand-mark" aria-hidden="true">
            <i />
            <i />
            <i />
          </span>
          sora
        </Link>
        <button
          ref={toggle}
          type="button"
          className="nav-toggle"
          aria-label={open ? '关闭导航菜单' : '打开导航菜单'}
          aria-expanded={open}
          aria-controls="site-navigation"
          onClick={() => setOpen((value) => !value)}
        >
          <span className="menu-icon" aria-hidden="true">
            <i />
            <i />
          </span>
        </button>
        <nav id="site-navigation" aria-label="主导航" data-open={open}>
          {navigation.map(([path, label]) => (
            <NavLink
              end={path !== '/funds'}
              to={path}
              key={path}
              onClick={() => navigate(path)}
            >
              {label}
              <span className="nav-arrow" aria-hidden="true">
                →
              </span>
            </NavLink>
          ))}
        </nav>
        <span className="demo-badge">
          <span className="status-dot" />
          示例预览
        </span>
      </div>
    </header>
  );
}
