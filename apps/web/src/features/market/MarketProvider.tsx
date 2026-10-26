import {
  createContext,
  useContext,
  useEffect,
  useState,
  type PropsWithChildren,
} from 'react';
import { marketSnapshotSchema, type MarketSnapshot } from './model';
import './market.css';

export const mockEnabled =
  import.meta.env.MODE === 'development' ||
  import.meta.env.VITE_MARKET_DATA === 'mock';
export type MarketLoader = () => Promise<unknown>;
const defaultLoader: MarketLoader | undefined = mockEnabled
  ? async () => (await import('./mock')).mockMarket
  : undefined;
type State = {
  enabled: boolean;
  status: 'idle' | 'loading' | 'ready' | 'error';
  data: MarketSnapshot | null;
  retry: () => void;
};
const Context = createContext<State>({
  enabled: false,
  status: 'idle',
  data: null,
  retry: () => {},
});
export const useMarket = () => useContext(Context);
export function MarketProvider({
  children,
  loader,
  enabled = mockEnabled,
}: PropsWithChildren<{ loader?: MarketLoader; enabled?: boolean }>) {
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<Pick<State, 'status' | 'data'>>({
    status: enabled ? 'loading' : 'idle',
    data: null,
  });
  useEffect(() => {
    if (!enabled) return;
    let active = true;
    const run = async () => {
      setResult((previous) => ({ ...previous, status: 'loading' }));
      try {
        const load = loader ?? defaultLoader;
        if (!load) throw new Error('No market data source configured');
        const data = marketSnapshotSchema.parse(await load());
        if (active) setResult({ status: 'ready', data });
      } catch {
        if (active) setResult((previous) => ({ ...previous, status: 'error' }));
      }
    };
    void run();
    return () => {
      active = false;
    };
  }, [attempt, enabled, loader]);
  return (
    <Context.Provider
      value={{
        enabled,
        ...(enabled ? result : { status: 'idle' as const, data: null }),
        retry: () => setAttempt((value) => value + 1),
      }}
    >
      {children}
    </Context.Provider>
  );
}
export function MarketNotice() {
  const market = useMarket();
  if (!market.enabled) return null;
  return (
    <div className="market-notice">
      <span className="market-demo-label">演示行情</span>
      <span>价格与走势为 Mock 数据，仅供页面体验。</span>
      {market.status === 'loading' && <span role="status">正在载入行情…</span>}
      {market.status === 'error' && (
        <span role="alert">
          行情暂时无法载入。
          <button type="button" onClick={market.retry}>
            重试行情
          </button>
        </span>
      )}
    </div>
  );
}
