import { useEffect, useRef } from 'react';
import { Link, useLocation } from 'react-router';
import { useAuth } from './AuthProvider';
import { loginReturn } from './navigation';

export function AccountControls() {
  const auth = useAuth();
  const location = useLocation();
  const menu = useRef<HTMLDetailsElement>(null);
  useEffect(() => {
    if (menu.current) menu.current.open = false;
  }, [location.pathname]);
  const next = new URLSearchParams({
    next: loginReturn(location.pathname + location.search),
  });
  if (auth.status === 'loading')
    return <span role="status">正在确认登录…</span>;
  if (auth.user)
    return (
      <details
        className="account-menu"
        aria-label="账号菜单"
        ref={menu}
        onKeyDown={(event) => {
          if (event.key === 'Escape' && menu.current) {
            menu.current.open = false;
            menu.current.querySelector('summary')?.focus();
            event.stopPropagation();
          }
        }}
      >
        <summary>我的账号</summary>
        <div className="account-menu-panel">
          <Link className="header-login" to="/account">
            账号设置
          </Link>
          {auth.user.role === 'admin' && (
            <Link className="header-login" to="/admin">
              管理后台
            </Link>
          )}
        </div>
      </details>
    );
  if (auth.status === 'error')
    return (
      <Link className="header-login" to="/account" title={auth.error}>
        账号连接异常
      </Link>
    );
  return (
    <>
      <Link className="header-login" to={`/login?${next}`}>
        登录
      </Link>
      <Link className="header-register" to={`/register?${next}`}>
        注册
      </Link>
    </>
  );
}
