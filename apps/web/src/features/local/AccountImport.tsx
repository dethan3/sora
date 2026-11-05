import { useEffect, useRef, useState } from 'react';
import type {
  ImportPreview,
  PersonalState,
  importChoicesSchema,
  z,
} from '@sora/market-data/personal';
import { ConfirmDialog } from '../../components/ConfirmDialog';
import { money } from '../portfolio/model';
import { usePersonal } from './PersonalProvider';
import { downloadText } from './download';
export function AccountImportDialog({
  source,
  onClose,
}: {
  source: PersonalState;
  onClose: () => void;
}) {
  const personal = usePersonal(),
    [preview, setPreview] = useState<ImportPreview>(),
    [choices, setChoices] = useState<z.infer<typeof importChoicesSchema>>({}),
    [loading, setLoading] = useState(true),
    [error, setError] = useState('');
  const alive = useRef(true);
  const generation = useRef(0);
  async function check() {
    const current = ++generation.current;
    setLoading(true);
    setError('');
    try {
      const result = await personal.previewImport(source);
      if (alive.current && current === generation.current) {
        setPreview(result);
        setChoices({});
      }
    } catch (cause) {
      if (alive.current && current === generation.current)
        setError(cause instanceof Error ? cause.message : '无法检查导入数据');
    } finally {
      if (alive.current && current === generation.current) setLoading(false);
    }
  }
  useEffect(() => {
    alive.current = true;
    void check();
    return () => {
      alive.current = false;
    };
  }, []);
  const eligibleCount =
    (preview?.eligible.watchlist.length ?? 0) +
    (preview?.eligible.positions.length ?? 0);
  return (
    <ConfirmDialog
      title="导入关注与持仓到当前账号"
      onCancel={onClose}
      busy={personal.busy}
    >
      <p>
        关注会合并去重；同一基金的持仓请选择保留哪份。原本地记录和备份文件均保留。
      </p>
      <button
        className="button-quiet"
        onClick={() =>
          downloadText(
            JSON.stringify(source, null, 2),
            'sora-before-import.json',
          )
        }
      >
        下载本次导入原件
      </button>
      {loading && <p role="status">正在核对公共目录和账号数据…</p>}
      {error && <p role="alert">{error}</p>}
      {preview && !loading && (
        <>
          <p>
            可导入 {preview.eligible.watchlist.length} 项关注、
            {preview.eligible.positions.length} 笔持仓。
          </p>
          {!!preview.unavailable.length && (
            <div className="local-warning">
              <strong>以下记录不会上传，原件保留：</strong>
              <ul>
                {preview.unavailable.map((row) => (
                  <li key={`${row.part}:${row.code}`}>
                    {row.code} · {row.part === 'position' ? '持仓' : '关注'} ·{' '}
                    {row.reason}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {preview.conflicts.map((row) => (
            <fieldset key={row.code} className="personal-import-conflict">
              <legend>{row.code} 持仓冲突</legend>
              <p>
                本次导入：{row.local.units} 份，总成本{' '}
                {money(row.local.costMinor, row.local.currency)}
              </p>
              <p>
                账号已有：{row.cloud.units} 份，总成本{' '}
                {money(row.cloud.costMinor, row.cloud.currency)}
              </p>
              <label htmlFor={`choice-${row.code}`}>
                保留 {row.code} 的哪份持仓
              </label>
              <select
                id={`choice-${row.code}`}
                value={choices[row.code] ?? ''}
                disabled={personal.busy}
                onChange={(event) =>
                  setChoices({
                    ...choices,
                    [row.code]: event.target.value as 'local' | 'cloud',
                  })
                }
              >
                <option value="" disabled>
                  请选择
                </option>
                <option value="cloud">保留账号已有持仓</option>
                <option value="local">使用本次导入持仓</option>
              </select>
            </fieldset>
          ))}
          {personal.error && <p role="alert">{personal.error}</p>}
          <div className="button-row">
            <button
              className="button-secondary"
              disabled={loading || personal.busy}
              onClick={() => void check()}
            >
              重新检查
            </button>
            <button
              className="action"
              disabled={
                personal.busy ||
                !eligibleCount ||
                preview.conflicts.some((row) => !choices[row.code]) ||
                personal.status === 'conflict'
              }
              onClick={async () => {
                if (await personal.importData(preview, choices)) onClose();
              }}
            >
              {personal.busy ? '正在导入…' : '确认导入可用记录'}
            </button>
          </div>
        </>
      )}
    </ConfirmDialog>
  );
}
export function AccountImportOffer() {
  const personal = usePersonal(),
    [open, setOpen] = useState(false);
  const local = personal.device;
  if (personal.mode !== 'account') return null;
  if (local.issue)
    return (
      <p className="muted">
        本浏览器的原本地记录暂不可读，未自动导入或覆盖。请退出账号后在本地数据管理中恢复备份。
      </p>
    );
  if (!local.state.watchlist.length && !local.state.positions.length)
    return null;
  return (
    <section className="local-warning" aria-label="导入原本地数据">
      <p>
        本浏览器另有 {local.state.watchlist.length} 项本地关注、
        {local.state.positions.length} 笔本地持仓，尚未自动上传。
      </p>
      <button
        className="button-secondary"
        disabled={personal.busy || personal.status === 'loading'}
        onClick={() => setOpen(true)}
      >
        查看并导入本地数据
      </button>
      {open && (
        <AccountImportDialog
          source={local.state}
          onClose={() => setOpen(false)}
        />
      )}
    </section>
  );
}
