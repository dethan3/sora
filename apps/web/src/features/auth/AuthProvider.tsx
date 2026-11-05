import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type PropsWithChildren,
} from 'react';
import { authRequest, identitySchema, type AuthUser } from './api';

type Status = 'loading' | 'ready' | 'error';
type AuthContext = {
  enabled: boolean;
  user: AuthUser | null;
  status: Status;
  error: string;
  refresh: () => Promise<void>;
  authenticate: (
    mode: 'login' | 'register',
    body: Record<string, string>,
  ) => Promise<void>;
  logout: () => Promise<void>;
};
const missing = async () => {
  throw new Error('请使用已启用账号服务的版本');
};
const Context = createContext<AuthContext>({
  enabled: false,
  user: null,
  status: 'ready',
  error: '',
  refresh: missing,
  authenticate: missing,
  logout: missing,
});
export const useAuth = () => useContext(Context);

export function AuthProvider({
  children,
  enabled = import.meta.env.VITE_AUTH_ENABLED === 'true',
}: PropsWithChildren<{ enabled?: boolean }>) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [status, setStatus] = useState<Status>(enabled ? 'loading' : 'ready');
  const [error, setError] = useState('');
  const generation = useRef(0);
  const changing = useRef(false);
  const refresh = useCallback(async () => {
    if (!enabled || changing.current) return;
    const current = ++generation.current;
    try {
      const result = await authRequest('/api/auth/me', identitySchema);
      if (current !== generation.current) return;
      setUser(result.user);
      setStatus('ready');
      setError('');
    } catch {
      if (current !== generation.current) return;
      setUser(null);
      setStatus('error');
      setError('暂时无法确认登录状态，请重试');
    }
  }, [enabled]);
  useEffect(() => {
    void refresh();
    const visible = () => {
      if (document.visibilityState === 'visible') void refresh();
    };
    const focus = () => void refresh();
    window.addEventListener('focus', focus);
    document.addEventListener('visibilitychange', visible);
    return () => {
      generation.current++;
      window.removeEventListener('focus', focus);
      document.removeEventListener('visibilitychange', visible);
    };
  }, [refresh]);
  async function authenticate(
    mode: 'login' | 'register',
    body: Record<string, string>,
  ) {
    if (!enabled || changing.current)
      throw new Error('账号请求正在处理中，请稍后重试');
    changing.current = true;
    const current = ++generation.current;
    try {
      const result = await authRequest(
        `/api/auth/${mode}`,
        identitySchema,
        body,
      );
      if (!result.user) throw new Error('登录结果无效，请重试');
      if (current === generation.current) {
        setUser(result.user);
        setStatus('ready');
        setError('');
      }
    } finally {
      changing.current = false;
    }
  }
  async function logout() {
    if (!enabled || changing.current)
      throw new Error('账号请求正在处理中，请稍后重试');
    changing.current = true;
    const current = ++generation.current;
    try {
      await authRequest('/api/auth/logout', identitySchema, {});
      if (current === generation.current) {
        setUser(null);
        setStatus('ready');
        setError('');
      }
    } finally {
      changing.current = false;
    }
  }
  return (
    <Context.Provider
      value={{ enabled, user, status, error, refresh, authenticate, logout }}
    >
      {children}
    </Context.Provider>
  );
}
