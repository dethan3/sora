import type { Fund } from '../data/demo';
import { Change, EmptyState } from './ui';
import { Link } from 'react-router';
import { WatchButton } from '../features/funds/WatchButton';

export function FundTable({ funds }: { funds: Fund[] }) {
  if (!funds.length)
    return (
      <EmptyState title="暂无基金">试试其他关键词，或稍后再来。</EmptyState>
    );
  return (
    <div className="table-scroll responsive-list">
      <table className="responsive-table fund-table" role="table">
        <caption className="sr-only">基金示例列表</caption>
        <thead role="rowgroup">
          <tr role="row">
            <th role="columnheader" scope="col">
              基金 / 代码
            </th>
            <th role="columnheader" scope="col">
              市场
            </th>
            <th role="columnheader" scope="col">
              类型
            </th>
            <th role="columnheader" scope="col" className="numeric">
              示例日变化
            </th>
            <th role="columnheader" scope="col" className="numeric">
              关注
            </th>
          </tr>
        </thead>
        <tbody role="rowgroup">
          {funds.map((fund) => (
            <tr role="row" key={fund.code}>
              <th role="rowheader" scope="row">
                <Link className="fund-name" to={`/funds/${fund.code}`}>
                  {fund.name}
                </Link>
                <small>{fund.code}</small>
              </th>
              <td role="cell">
                <span className="mobile-field-label" aria-hidden="true">
                  市场
                </span>
                {fund.market}
              </td>
              <td role="cell">
                <span className="mobile-field-label" aria-hidden="true">
                  类型与币种
                </span>
                {fund.category}
                <small>{fund.currency} 计价</small>
              </td>
              <td role="cell" className="numeric">
                <span className="mobile-field-label" aria-hidden="true">
                  示例日变化
                </span>
                {fund.nav === null ? (
                  <span className="muted">暂无净值</span>
                ) : (
                  <Change value={fund.changePct} />
                )}
              </td>
              <td role="cell" className="numeric card-actions">
                <WatchButton fund={fund} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
