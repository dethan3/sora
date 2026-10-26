import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  type PropsWithChildren,
} from 'react';
import { useLocation } from 'react-router';
import { z } from 'zod';
import { ConfirmDialog } from '../../components/ConfirmDialog';
import { downloadText } from '../local/BackupTools';

export const DIRECTION_KEY = 'sora.directions.v1';
export const directionsSchema = z
  .object({
    schemaVersion: z.literal(1),
    ids: z.array(z.string().regex(/^[a-z0-9-]{1,50}$/)).max(100),
  })
  .strict()
  .refine((value) => new Set(value.ids).size === value.ids.length);
type Snapshot = {
  ids: string[];
  raw: string | null;
  issue: 'corrupt' | 'unavailable' | null;
};
export function readDirections(): Snapshot {
  let raw: string | null;
  try {
    raw = localStorage.getItem(DIRECTION_KEY);
  } catch {
    return { ids: [], raw: null, issue: 'unavailable' };
  }
  if (raw === null) return { ids: [], raw, issue: null };
  try {
    if (raw.length > 20000) throw new Error();
    return {
      ids: directionsSchema.parse(JSON.parse(raw)).ids,
      raw,
      issue: null,
    };
  } catch {
    return { ids: [], raw, issue: 'corrupt' };
  }
}
type Store = Snapshot & {
  error: string;
  notice: string;
  canUndo: boolean;
  toggle: (id: string, name: string) => void;
  undo: () => void;
  reset: () => void;
};
const Context = createContext<Store | null>(null);
export function DirectionProvider({ children }: PropsWithChildren) {
  const [snapshot, setSnapshot] = useState(readDirections);
  const current = useRef(snapshot);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [undoState, setUndoState] = useState<{ ids: string[]; raw: string }>();
  const location = useLocation();
  useEffect(() => {
    setError('');
    setNotice('');
    setUndoState(undefined);
  }, [location.pathname]);
  useEffect(() => {
    const sync = (event: StorageEvent) => {
      if (event.key !== DIRECTION_KEY && event.key !== null) return;
      const next = readDirections();
      current.current = next;
      setSnapshot(next);
      setError('');
      setNotice('已同步其他页面的观察方向。');
      setUndoState(undefined);
    };
    window.addEventListener('storage', sync);
    return () => window.removeEventListener('storage', sync);
  }, []);
  function write(
    ids: string[],
    message: string,
    recover = false,
  ): string | null {
    setError('');
    setNotice('');
    setUndoState(undefined);
    if (current.current.issue && !recover) {
      setError('观察方向未保存，请先处理本地存储问题。');
      return null;
    }
    try {
      if (localStorage.getItem(DIRECTION_KEY) !== current.current.raw) {
        setError('其他页面已更新观察方向，请刷新后重试。');
        return null;
      }
      const raw = JSON.stringify(
        directionsSchema.parse({ schemaVersion: 1, ids }),
      );
      localStorage.setItem(DIRECTION_KEY, raw);
      const next = { ids, raw, issue: null };
      current.current = next;
      setSnapshot(next);
      setNotice(message);
      return raw;
    } catch {
      setError('无法保存观察方向，修改未生效。请检查浏览器存储权限或空间。');
      return null;
    }
  }
  function toggle(id: string, name: string) {
    const previous = current.current.ids;
    const removing = previous.includes(id);
    const raw = write(
      removing ? previous.filter((item) => item !== id) : [...previous, id],
      removing ? `已取消关注${name}。` : `已关注${name}。`,
    );
    if (raw) setUndoState(removing ? { ids: previous, raw } : undefined);
  }
  function undo() {
    if (!undoState) return;
    if (current.current.raw !== undoState.raw) {
      setError('观察方向已发生变化，无法撤销旧操作。');
      setUndoState(undefined);
      return;
    }
    if (write(undoState.ids, '已恢复关注。')) setUndoState(undefined);
  }
  return (
    <Context.Provider
      value={{
        ...snapshot,
        error,
        notice,
        canUndo: !!undoState,
        toggle,
        undo,
        reset: () => {
          if (write([], '已重置观察方向。', true)) setUndoState(undefined);
        },
      }}
    >
      {children}
    </Context.Provider>
  );
}
export function useDirections() {
  const value = useContext(Context);
  if (!value) throw new Error('DirectionProvider missing');
  return value;
}
export function DirectionNotice() {
  const store = useDirections();
  const [confirm, setConfirm] = useState(false);
  return (
    <>
      {store.issue && (
        <div className="local-warning" role="alert">
          <strong>
            {store.issue === 'corrupt'
              ? '无法读取观察方向，原始记录已保留。'
              : '当前浏览器无法访问观察方向的本地存储。'}
          </strong>
          {store.issue === 'corrupt' && (
            <div className="button-row">
              <button
                className="button-quiet"
                onClick={() =>
                  downloadText(store.raw!, 'sora-directions-recovery.json')
                }
              >
                下载原始观察记录
              </button>
              <button className="button-quiet" onClick={() => setConfirm(true)}>
                重置观察方向
              </button>
            </div>
          )}
        </div>
      )}
      {store.error && (
        <p className="form-error direction-notice direction-error" role="alert">
          {store.error}
        </p>
      )}
      {store.notice && (
        <div className="direction-notice" role="status">
          <span>{store.notice}</span>
          {store.canUndo && (
            <button className="button-quiet" onClick={store.undo}>
              撤销
            </button>
          )}
        </div>
      )}
      {confirm && (
        <ConfirmDialog
          title="重置观察方向？"
          confirmLabel="重置"
          onCancel={() => setConfirm(false)}
          onConfirm={() => {
            store.reset();
            setConfirm(false);
          }}
        >
          请先下载需要保留的原始记录。重置仅影响观察方向，基金关注与持仓保持不变。
        </ConfirmDialog>
      )}
    </>
  );
}
