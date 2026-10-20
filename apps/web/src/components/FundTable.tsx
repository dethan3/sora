import type { Fund } from '../data/demo';
import { Change, EmptyState } from './ui';

export function FundTable({ funds }: { funds: Fund[] }) {
  if (!funds.length)
    return (
      <EmptyState title="暂无基金">试试其他关键词，或稍后再来。</EmptyState>
    );
  return (
    <div className="table-scroll">
      <table>
        <caption className="sr-only">基金示例列表</caption>
        <thead>
          <tr>
            <th scope="col">基金 / 代码</th>
            <th scope="col">市场</th>
            <th scope="col">类型</th>
            <th scope="col" className="numeric">
              示例日变化
            </th>
          </tr>
        </thead>
        <tbody>
          {funds.map((fund) => (
            <tr key={fund.code}>
              <th scope="row">
                <span className="fund-name">{fund.name}</span>
                <small>{fund.code}</small>
              </th>
              <td>{fund.market}</td>
              <td>{fund.category}</td>
              <td className="numeric">
                <Change value={fund.changePct} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
