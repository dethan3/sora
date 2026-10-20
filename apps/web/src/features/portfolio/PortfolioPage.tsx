import { useState } from 'react';
import { Link, useSearchParams } from 'react-router';
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

export function PortfolioPage({ data }: { data: DemoData }) {
  const local = usePersonal();
  const [params, setParams] = useSearchParams();
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
  const [loadDemo, setLoadDemo] = useState(false);
  const [error, setError] = useState('');
  const groups = summarizePositions(local.state.positions, data.funds);
  function closeEditor() {
    setEditor(null);
    setError('');
    document.getElementById('add-position')?.focus();
    if (params.has('add')) {
      const next = new URLSearchParams(params);
      next.delete('add');
      setParams(next, { replace: true });
    }
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
    if (local.commit({ ...local.state, positions })) {
      closeEditor();
      return true;
    }
    return false;
  }
  return (
    <>
      <div className="portfolio-heading">
        <PageHeader
          eyebrow="YOUR PORTFOLIO"
          title="我的组合"
          description="录入份额与成本，按虚构样本净值查看估值。不同币种分别汇总，不做汇率换算。"
        />
        <button
          id="add-position"
          className="action"
          disabled={!!local.issue}
          onClick={() => {
            setError('');
            setEditor({});
          }}
        >
          + 录入持仓
        </button>
      </div>
      {editor && (
        <PositionForm
          key={editor.original?.code ?? editor.code ?? 'new'}
          funds={data.funds}
          position={editor.original}
          defaultCode={editor.code}
          onSave={save}
          onCancel={closeEditor}
          submitError={error}
        />
      )}
      {error && !editor && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      {groups.map((group) => (
        <section
          className="currency-group"
          key={group.currency}
          aria-label={`${group.currency} 组合汇总`}
        >
          <div className="section-heading">
            <h2>{group.currency} 组合</h2>
            <span>估值日期 {data.asOf} · 虚构净值</span>
          </div>
          <div className="summary-grid">
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
      <Panel title={`我的持仓 · ${local.state.positions.length}`}>
        {local.state.positions.length ? (
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
                    样本估值
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
                {local.state.positions.map((position) => {
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
                            to={`/funds/${fund.code}`}
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
                          样本估值
                        </span>
                        {value === null ? (
                          <span className="valuation-missing">待估值</span>
                        ) : (
                          money(value, position.currency)
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
                            disabled={!!local.issue}
                            onClick={() => {
                              setError('');
                              setEditor({ original: position });
                            }}
                          >
                            编辑
                            <span className="sr-only">
                              {fund?.name ?? position.code}
                            </span>
                          </button>
                          <button
                            className="button-quiet danger"
                            disabled={!!local.issue}
                            onClick={() => setPendingDelete(position)}
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
            <br />
            也可以先载入演示持仓体验。
          </EmptyState>
        )}
        <div className="portfolio-footnote">
          <p>
            来源：Sora
            虚构净值样本。估值不含分红、交易费用和汇率；缺失净值不计作零。
          </p>
          <button
            className="button-quiet"
            disabled={!!local.issue}
            onClick={() => setLoadDemo(true)}
          >
            载入演示持仓
          </button>
        </div>
      </Panel>
      <BackupTools />
      {pendingDelete && (
        <ConfirmDialog
          title="删除这笔持仓？"
          confirmLabel="确认删除"
          onCancel={() => setPendingDelete(undefined)}
          onConfirm={() => {
            if (
              local.commit(
                {
                  ...local.state,
                  positions: local.state.positions.filter(
                    (position) => position.code !== pendingDelete.code,
                  ),
                },
                '持仓已删除。',
              )
            ) {
              if (editor?.original?.code === pendingDelete.code) closeEditor();
              setPendingDelete(undefined);
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
