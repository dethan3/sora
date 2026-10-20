import { useSearchParams } from 'react-router';
import type { DemoData } from '../../data/demo';
import { FundTable } from '../../components/FundTable';
import { PageHeader, Panel } from '../../components/ui';

export function FundsPage({ data }: { data: DemoData }) {
  const [params, setParams] = useSearchParams();
  const query = params.get('q') ?? '';
  const moduleId = params.get('module') ?? '';
  const module = data.modules.find((item) => item.id === moduleId);
  const funds = data.funds.filter(
    (fund) =>
      (!moduleId || !!module?.fundCodes.includes(fund.code)) &&
      `${fund.name} ${fund.code} ${fund.market}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
  );
  function update(key: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    setParams(next, { replace: true });
  }
  return (
    <>
      <PageHeader
        eyebrow="FUND LIBRARY"
        title="基金"
        description="查找基金、查看样本趋势，建立自己的关注列表。"
      />
      <Panel title="基金目录">
        <div className="filter-row">
          <div>
            <label className="search-label" htmlFor="fund-search">
              搜索示例基金
            </label>
            <input
              id="fund-search"
              type="search"
              value={query}
              onChange={(event) => update('q', event.target.value)}
              placeholder="名称、代码或市场"
            />
          </div>
          <div>
            <label className="search-label" htmlFor="module-filter">
              地区与主题
            </label>
            <select
              id="module-filter"
              value={moduleId}
              onChange={(event) => update('module', event.target.value)}
            >
              <option value="">全部地区与主题</option>
              {moduleId && !module && (
                <option value={moduleId}>模块已不存在</option>
              )}
              {data.modules.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.title}
                </option>
              ))}
            </select>
          </div>
          <span className="result-count">{funds.length} 只基金</span>
        </div>
        <FundTable funds={funds} />
      </Panel>
    </>
  );
}
