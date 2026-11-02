import { useEffect, useRef, useState } from 'react';
import { Link, useLocation, useSearchParams } from 'react-router';
import type { DemoData } from '../../data/demo';
import { Change, EmptyState, PageHeader, Panel } from '../../components/ui';
import { ConfirmDialog } from '../../components/ConfirmDialog';
import { usePersonal } from '../local/PersonalProvider';
import { BackupTools } from '../local/BackupTools';
import {
  currencyDigits,
  money,
  summarizePositions,
  valuePosition,
  type Position,
} from './model';
import { PositionForm } from './PositionForm';
import {
  detailSource,
  detailUrl,
  editPositionId,
  fundLinkId,
  rememberList,
} from '../../navigation/flow';
import { useUnsavedChanges } from '../../navigation/useUnsavedChanges';

export function PortfolioPage({ data }: { data: DemoData }) {
  const local = usePersonal();
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const source = detailSource(params.get('from'));
  const requestedCode = params.get('add');
  const [editor, setEditor] = useState<{
    original?: Position;
    code?: string;
  } | null>(
    requestedCode && data.funds.some((f) => f.code === requestedCode)
      ? local.state.positions.some(
          (position) => position.code === requestedCode,
        )
        ? {
            original: local.state.positions.find(
              (position) => position.code === requestedCode,
            )!,
          }
        : { code: requestedCode }
      : null,
  );
  const [pendingDelete, setPendingDelete] = useState<Position>();
  const [editorSession, setEditorSession] = useState(0);
  const [loadDemo, setLoadDemo] = useState(false);
  const [error, setError] = useState('');
  const [dirty, setDirty] = useState(false);
  const unsaved = useUnsavedChanges(dirty);
  const editorTrigger = useRef<HTMLElement | null>(null);
  const focusAfterChange = useRef<string | null>(null);
  const groups = summarizePositions(local.state.positions, data.funds);
  const currency =
    groups.find((group) => group.currency === params.get('currency'))
      ?.currency ?? groups[0]?.currency;
  const visiblePositions = local.state.positions.filter(
    (position) => position.currency === currency,
  );
  useEffect(() => {
    if (editor && local.error) {
      const errorElement = document.getElementById('personal-save-error');
      errorElement?.focus();
      errorElement?.scrollIntoView({ block: 'nearest' });
    }
  }, [editor, local.error]);
  useEffect(() => {
    if (!editor && focusAfterChange.current) {
      (
        document.getElementById(focusAfterChange.current) ??
        document.getElementById('add-position')
      )?.focus();
      focusAfterChange.current = null;
    }
  }, [editor, local.state.positions]);
  function closeEditor(savedCode?: string, savedCurrency?: string) {
    setEditor(null);
    setDirty(false);
    setError('');
    if (savedCode) focusAfterChange.current = editPositionId(savedCode);
    else if (editor && editorTrigger.current?.isConnected)
      editorTrigger.current.focus();
    else if (editor) document.getElementById('add-position')?.focus();
    if (params.has('add') || savedCurrency) {
      const next = new URLSearchParams(params);
      next.delete('add');
      if (savedCurrency) next.set('currency', savedCurrency);
      unsaved.approved(() => setParams(next, { replace: true }));
    }
  }
  function openEditor(code?: string, trigger?: HTMLElement) {
    unsaved.request(() => {
      const existing = local.state.positions.find(
        (position) => position.code === code,
      );
      editorTrigger.current = trigger ?? editorTrigger.current;
      setDirty(false);
      setError('');
      setEditor(existing ? { original: existing } : code ? { code } : {});
      setEditorSession((value) => value + 1);
      const next = new URLSearchParams(params);
      if (code) next.set('add', code);
      else next.delete('add');
      unsaved.approved(() => setParams(next, { replace: true }));
    });
  }
  function save(position: Position) {
    setError('');
    const existing = local.state.positions.find(
      (item) => item.code === position.code,
    );
    if (!editor?.original && existing) {
      setError('这只基金已有持仓，请编辑现有记录。');
      return false;
    }
    if (
      editor?.original &&
      JSON.stringify(existing) !== JSON.stringify(editor.original)
    ) {
      setError('这笔持仓已在其他页面更改，请取消编辑后重新打开。');
      return false;
    }
    const positions = editor?.original
      ? local.state.positions.map((item) =>
          item.code === position.code ? position : item,
        )
      : [...local.state.positions, position];
    const name =
      data.funds.find((fund) => fund.code === position.code)?.name ??
      position.code;
    if (
      local.commit(
        { ...local.state, positions },
        `已保存${name}，保存在此浏览器。`,
      )
    ) {
      closeEditor(position.code, position.currency);
      return true;
    }
    return false;
  }
  return (
    <>
      {source && (
        <Link className="back-link" to={source}>
          ← 返回基金详情
        </Link>
      )}
      <div className="portfolio-heading">
        <PageHeader
          eyebrow="YOUR PORTFOLIO"
          title="我的组合"
          description="记录份额与成本，清楚回看每笔持仓。不同币种分别计算。"
        />
        <button
          id="add-position"
          className="action"
          disabled={!!local.issue}
          onClick={(event) => openEditor(undefined, event.currentTarget)}
        >
          + 录入持仓
        </button>
      </div>
      {editor && (
        <PositionForm
          key={`${editor.original?.code ?? editor.code ?? 'new'}:${editorSession}`}
          funds={data.funds}
          position={editor.original}
          defaultCode={editor.code}
          onSave={save}
          onCancel={() => unsaved.request(() => closeEditor())}
          submitError={error}
          onDirtyChange={setDirty}
          onSelectFund={(code) => openEditor(code)}
          heldCodes={local.state.positions.map((position) => position.code)}
        />
      )}
      {error && !editor && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      {requestedCode &&
        !data.funds.some((fund) => fund.code === requestedCode) && (
          <p className="form-error" role="alert">
            目录中未找到这只基金，请从基金目录重新选择。
            <Link to="/funds">浏览基金 →</Link>
          </p>
        )}
      {groups.length > 0 && (
        <div className="currency-switch" role="group" aria-label="组合币种">
          {groups.map((group) => (
            <button
              key={group.currency}
              type="button"
              aria-pressed={currency === group.currency}
              onClick={() =>
                unsaved.request(() => {
                  setEditor(null);
                  setDirty(false);
                  setError('');
                  const next = new URLSearchParams(params);
                  next.delete('add');
                  next.set('currency', group.currency);
                  unsaved.approved(() => setParams(next, { replace: true }));
                })
              }
            >
              {
                { CNY: '人民币', USD: '美元', HKD: '港元', KRW: '韩元' }[
                  group.currency
                ]
              }{' '}
              {group.currency}
              <span>
                {
                  local.state.positions.filter(
                    (position) => position.currency === group.currency,
                  ).length
                }{' '}
                笔
              </span>
            </button>
          ))}
        </div>
      )}
      {groups
        .filter((group) => group.currency === currency)
        .map((group) => (
          <section
            className="currency-group"
            key={group.currency}
            aria-label={`${group.currency} 组合汇总`}
          >
            <div className="section-heading">
              <h2>{group.currency} 组合</h2>
              <span>
                {data.source === 'reference-catalog'
                  ? '暂无净值数据，持仓待估值'
                  : '净值日期 ' + data.asOf}
              </span>
            </div>
            <div className="summary-grid portfolio-summary">
              <Panel title={`总市值 · ${group.currency}`}>
                <p className="metric">
                  {group.value === null
                    ? '待完整估值'
                    : money(group.value, group.currency)}
                </p>
                {group.missing > 0 && (
                  <p className="field-help">
                    {group.missing} 笔缺少可用净值；已知市值{' '}
                    {money(group.knownValue, group.currency)}
                  </p>
                )}
              </Panel>
              <Panel title="总投入成本">
                <p className="metric">{money(group.cost, group.currency)}</p>
              </Panel>
              <Panel title="持有收益">
                <p className="metric">
                  {group.profit === null
                    ? '—'
                    : money(group.profit, group.currency)}
                </p>
                {group.changePct === null ? (
                  <p className="field-help">
                    {group.value === null
                      ? '净值不完整，暂不计算收益'
                      : '零成本，收益率不适用'}
                  </p>
                ) : (
                  <Change value={group.changePct} />
                )}
              </Panel>
            </div>
          </section>
        ))}
      <Panel
        title={`我的持仓${currency ? ` · ${currency}` : ''} · ${visiblePositions.length}`}
      >
        {visiblePositions.length ? (
          <div className="table-scroll responsive-list">
            <table className="responsive-table holding-table" role="table">
              <caption className="sr-only">本地持仓列表</caption>
              <thead role="rowgroup">
                <tr role="row">
                  <th role="columnheader" scope="col">
                    基金 / 份额
                  </th>
                  <th role="columnheader" scope="col" className="numeric">
                    总成本
                  </th>
                  <th role="columnheader" scope="col" className="numeric">
                    持仓估值
                  </th>
                  <th role="columnheader" scope="col" className="numeric">
                    持有收益
                  </th>
                  <th role="columnheader" scope="col" className="numeric">
                    同币种占比
                  </th>
                  <th role="columnheader" scope="col" className="numeric">
                    操作
                  </th>
                </tr>
              </thead>
              <tbody role="rowgroup">
                {visiblePositions.map((position) => {
                  const fund = data.funds.find((f) => f.code === position.code);
                  const value = valuePosition(position, fund);
                  const group = groups.find(
                    (g) => g.currency === position.currency,
                  )!;
                  const weight =
                    group.value && value !== null
                      ? (value / group.value) * 100
                      : null;
                  return (
                    <tr role="row" key={position.code}>
                      <th role="rowheader" scope="row">
                        {fund ? (
                          <Link
                            className="fund-name"
                            id={fundLinkId(fund.code)}
                            to={detailUrl(
                              fund.code,
                              location.pathname + location.search,
                            )}
                            onClick={() =>
                              rememberList(
                                location.pathname + location.search,
                                fund.code,
                              )
                            }
                          >
                            {fund.name}
                          </Link>
                        ) : (
                          position.code
                        )}
                        <small>
                          {position.units} 份 · {position.currency}
                          {!fund || fund.currency !== position.currency
                            ? ' · 目录或币种已变化'
                            : ''}
                        </small>
                      </th>
                      <td role="cell" className="numeric">
                        <span className="mobile-field-label" aria-hidden="true">
                          总成本
                        </span>
                        {money(position.costMinor, position.currency)}
                      </td>
                      <td role="cell" className="numeric">
                        <span className="mobile-field-label" aria-hidden="true">
                          持仓估值
                        </span>
                        {value === null ? (
                          <span className="valuation-missing">待估值</span>
                        ) : (
                          money(value, position.currency)
                        )}
                      </td>
                      <td role="cell" className="numeric holding-profit">
                        <span className="mobile-field-label" aria-hidden="true">
                          持有收益
                        </span>
                        {value === null ? (
                          '—'
                        ) : (
                          <>
                            <span
                              className={
                                value - position.costMinor >= 0
                                  ? 'positive'
                                  : 'negative'
                              }
                            >
                              {money(
                                value - position.costMinor,
                                position.currency,
                              )}
                            </span>
                            <small>
                              {position.costMinor === 0 ? (
                                '零成本，不计算收益率'
                              ) : (
                                <Change
                                  value={
                                    ((value - position.costMinor) /
                                      position.costMinor) *
                                    100
                                  }
                                />
                              )}
                            </small>
                          </>
                        )}
                      </td>
                      <td role="cell" className="numeric holding-weight">
                        <span className="mobile-field-label" aria-hidden="true">
                          同币种占比
                        </span>
                        {weight === null ? (
                          '—'
                        ) : (
                          <div className="weight-cell">
                            <span
                              className="allocation-track"
                              aria-hidden="true"
                            >
                              <span style={{ width: `${weight}%` }} />
                            </span>
                            {weight.toFixed(1)}%
                          </div>
                        )}
                      </td>
                      <td role="cell" className="numeric card-actions">
                        <div className="row-actions">
                          <button
                            className="button-quiet"
                            id={editPositionId(position.code)}
                            disabled={!!local.issue}
                            onClick={(event) =>
                              openEditor(position.code, event.currentTarget)
                            }
                          >
                            编辑
                            <span className="sr-only">
                              {fund?.name ?? position.code}
                            </span>
                          </button>
                          <button
                            className="button-quiet danger"
                            disabled={!!local.issue}
                            onClick={() =>
                              unsaved.request(() => {
                                closeEditor();
                                setPendingDelete(position);
                              })
                            }
                          >
                            删除
                            <span className="sr-only">
                              {fund?.name ?? position.code}
                            </span>
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState title="从第一笔持仓开始">
            录入基金份额和总成本，建立你的本地组合。
            <div className="empty-actions">
              <button
                className="action"
                disabled={!!local.issue}
                onClick={(event) => openEditor(undefined, event.currentTarget)}
              >
                录入第一笔持仓
              </button>
              <Link className="text-link" to="/funds">
                先浏览基金 →
              </Link>
            </div>
          </EmptyState>
        )}
        <div className="portfolio-footnote">
          <p>
            估值基于可用的每份净值，不含分红、交易费用和汇率；缺失净值不计作零。资料来源见产品详情。
          </p>
        </div>
      </Panel>
      <details
        className="data-management"
        open={local.issue ? true : undefined}
      >
        <summary>备份与数据管理</summary>
        <BackupTools
          onBeforeReplace={(action) =>
            unsaved.request(() => {
              closeEditor();
              action();
            })
          }
        />
        {data.source === 'fictional-demo' && (
          <button
            className="button-quiet"
            disabled={!!local.issue}
            onClick={() =>
              unsaved.request(() => {
                closeEditor();
                setLoadDemo(true);
              })
            }
          >
            载入演示持仓
          </button>
        )}
      </details>
      {unsaved.dialog}
      {pendingDelete && (
        <ConfirmDialog
          title="删除这笔持仓？"
          confirmLabel="确认删除"
          onCancel={() => setPendingDelete(undefined)}
          onConfirm={() => {
            const remaining = local.state.positions.filter(
              (position) => position.code !== pendingDelete.code,
            );
            if (
              local.commit(
                {
                  ...local.state,
                  positions: remaining,
                },
                '持仓已删除。',
              )
            ) {
              const index = visiblePositions.findIndex(
                (position) => position.code === pendingDelete.code,
              );
              const neighbor =
                visiblePositions[index + 1] ?? visiblePositions[index - 1];
              focusAfterChange.current = neighbor
                ? editPositionId(neighbor.code)
                : 'add-position';
              setPendingDelete(undefined);
              if (
                !remaining.some((position) => position.currency === currency)
              ) {
                const next = new URLSearchParams(params);
                if (remaining.length)
                  next.set('currency', remaining[0].currency);
                else next.delete('currency');
                unsaved.approved(() => setParams(next, { replace: true }));
              }
            }
          }}
        >
          <p>
            {data.funds.find((f) => f.code === pendingDelete.code)?.name ??
              pendingDelete.code}
            ，{pendingDelete.units} 份。删除只影响此浏览器的记录。
          </p>
          {local.error && (
            <p role="alert" className="form-error">
              {local.error}
            </p>
          )}
        </ConfirmDialog>
      )}
      {loadDemo && (
        <ConfirmDialog
          title="用演示持仓替换当前持仓？"
          confirmLabel="载入演示"
          onCancel={() => setLoadDemo(false)}
          onConfirm={() => {
            const positions = data.positions.flatMap((row) => {
              const fund = data.funds.find((f) => f.code === row.code);
              return fund?.nav
                ? [
                    {
                      code: row.code,
                      units: (row.value / Number(fund.nav)).toFixed(4),
                      costMinor: Math.round(
                        row.cost * 10 ** currencyDigits(fund.currency),
                      ),
                      currency: fund.currency,
                    },
                  ]
                : [];
            });
            if (
              local.commit(
                { ...local.state, positions },
                '已载入虚构演示持仓。',
              )
            ) {
              closeEditor();
              setLoadDemo(false);
            }
          }}
        >
          <p>
            这会替换当前持仓，关注列表保持不变。演示持仓包含不同币种，分别展示。
          </p>
          {local.error && (
            <p role="alert" className="form-error">
              {local.error}
            </p>
          )}
        </ConfirmDialog>
      )}
    </>
  );
}
