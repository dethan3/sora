import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  type PropsWithChildren,
} from 'react';
import { useLocation } from 'react-router';
import {
  emptyPersonal,
  readPersonal,
  personalSchema,
  STORAGE_KEY,
  writePersonal,
  type PersonalState,
  type Snapshot,
} from './state';

type PersonalContext = {
  state: PersonalState;
  issue: Snapshot['issue'];
  error: string;
  notice: string;
  commit: (
    state: PersonalState,
    message?: string,
    recover?: boolean,
  ) => boolean;
  raw: string | null;
};
const Context = createContext<PersonalContext | null>(null);
function initial(): Snapshot {
  try {
    return readPersonal(window.localStorage);
  } catch {
    return { state: emptyPersonal(), raw: null, issue: 'unavailable' };
  }
}
export function PersonalProvider({ children }: PropsWithChildren) {
  const location = useLocation();
  const [snapshot, setSnapshot] = useState(initial);
  const current = useRef(snapshot);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  useEffect(() => {
    setError('');
    setNotice('');
  }, [location.pathname]);
  useEffect(() => {
    const update = (event: StorageEvent) => {
      if (event.key !== STORAGE_KEY && event.key !== null) return;
      const next = initial();
      current.current = next;
      setSnapshot(next);
      setError('');
      setNotice('已同步此浏览器其他页面的修改。');
    };
    window.addEventListener('storage', update);
    return () => window.removeEventListener('storage', update);
  }, []);
  function commit(
    state: PersonalState,
    message = '已保存到此浏览器。',
    recover = false,
  ) {
    setError('');
    setNotice('');
    if (current.current.issue && !recover) {
      setError('请先恢复本地数据，再进行修改。');
      return false;
    }
    if (!personalSchema.safeParse(state).success) {
      setError('数据未通过校验。关注与持仓分别最多支持 200 条，请检查输入。');
      return false;
    }
    try {
      const raw = writePersonal(
        window.localStorage,
        current.current.raw,
        state,
      );
      const next = { state, raw, issue: null };
      current.current = next;
      setSnapshot(next);
      setNotice(message);
      return true;
    } catch (cause) {
      setError(
        cause instanceof Error && cause.message.includes('另一个页面')
          ? cause.message
          : '无法保存到浏览器，修改未生效。请检查存储权限或空间后重试。',
      );
      return false;
    }
  }
  return (
    <Context.Provider
      value={{
        state: snapshot.state,
        raw: snapshot.raw,
        issue: snapshot.issue,
        commit,
        error,
        notice,
      }}
    >
      {children}
    </Context.Provider>
  );
}
export function usePersonal() {
  const value = useContext(Context);
  if (!value) throw new Error('PersonalProvider missing');
  return value;
}
