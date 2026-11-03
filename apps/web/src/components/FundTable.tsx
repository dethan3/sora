import type { Fund } from '../data/demo';
import { EmptyState } from './ui';
import { FundPrice } from '../features/market/FundPrice';
import { Link, useLocation } from 'react-router';
import { detailUrl, fundLinkId, rememberList } from '../navigation/flow';
import { WatchButton } from '../features/funds/WatchButton';
import {
  fundAccessProfile,
  fundIngredients,
  fundSnapshot,
} from '../features/domains/model';

export function FundTable({ funds }: { funds: Fund[] }) {
  const location = useLocation();
  const source = location.pathname + location.search;
  if (!funds.length)
    return (
      <EmptyState title="暂无基金">试试其他关键词，或稍后再来。</EmptyState>
    );
  return (
    <div className="table-scroll responsive-list">
      <table className="responsive-table fund-table" role="table">
        <caption className="sr-only">基金列表</caption>
        <thead role="rowgroup">
          <tr role="row">
            <th role="columnheader" scope="col">
              基金 / 代码
            </th>
            <th role="columnheader" scope="col">
              市场
            </th>
            <th role="columnheader" scope="col">
              类型与配料
            </th>
            <th role="columnheader" scope="col" className="numeric">
              收盘价 / 日变化
            </th>
            <th role="columnheader" scope="col" className="numeric">
              关注
            </th>
          </tr>
        </thead>
        <tbody role="rowgroup">
          {funds.map((fund) => {
            const snapshot = fundSnapshot(fund.code);
            const ingredients = snapshot
              ? fundIngredients(snapshot)
              : undefined;
            const profile = fundAccessProfile(fund.code);
            return (
              <tr role="row" key={fund.code}>
                <th role="rowheader" scope="row">
                  <Link
                    className="fund-name"
                    id={fundLinkId(fund.code)}
                    to={detailUrl(fund.code, source)}
                    onClick={() => rememberList(source, fund.code)}
                  >
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
                  {ingredients ? (
                    <div className="table-ingredient-tags">
                      <span className="mini-tag is-storage">
                        AI存储{' '}
                        {ingredients.storageWeight > 0
                          ? `${ingredients.storageWeight.toFixed(1)}%`
                          : '前十未见'}
                      </span>
                      <span className="mini-tag is-chips">
                        AI芯片 {ingredients.chipsWeight.toFixed(1)}%
                      </span>
                    </div>
                  ) : (
                    profile && (
                      <div className="table-ingredient-tags">
                        <span className="mini-tag">
                          {profile.ingredientTag}
                        </span>
                      </div>
                    )
                  )}
                </td>
                <td role="cell" className="numeric">
                  <span className="mobile-field-label" aria-hidden="true">
                    收盘价 / 日变化
                  </span>
                  <FundPrice fund={fund} />
                </td>
                <td role="cell" className="numeric card-actions">
                  <WatchButton fund={fund} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
