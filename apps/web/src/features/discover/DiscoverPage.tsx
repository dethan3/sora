import { Link } from 'react-router';
import type { DemoData } from '../../data/demo';
import { PageHeader } from '../../components/ui';
import { detailUrl, fundLinkId, rememberList } from '../../navigation/flow';

export function DiscoverPage({ data }: { data: DemoData }) {
  return (
    <>
      <PageHeader
        eyebrow="DISCOVER"
        title="发现市场"
        description="从地区与主题出发，找到值得持续了解的方向。"
      />
      {(['region', 'theme'] as const).map((kind) => (
        <section className="discovery-section" key={kind}>
          <div className="section-heading">
            <h2>{kind === 'region' ? '按地区发现' : '按主题发现'}</h2>
            <span>虚构样本 · 仅供体验</span>
          </div>
          <div className="market-grid">
            {data.modules
              .filter((module) => module.kind === kind)
              .map((module, index) => (
                <section
                  className={`market-card market-${index % 3}`}
                  key={module.id}
                >
                  <div className="market-card-top">
                    <span className="market-symbol" aria-hidden="true">
                      {module.symbol}
                    </span>
                    <span className="market-tag">
                      {module.fundCodes.length} 只示例基金
                    </span>
                  </div>
                  <h2>{module.title}</h2>
                  <p className="market-note">{module.description}</p>
                  <div className="module-funds">
                    {module.fundCodes.map((code) => (
                      <Link
                        to={detailUrl(code, '/discover')}
                        id={fundLinkId(`${module.id}:${code}`)}
                        key={code}
                        onClick={() =>
                          rememberList('/discover', `${module.id}:${code}`)
                        }
                      >
                        {data.funds.find((fund) => fund.code === code)?.name}
                        <span aria-hidden="true">↗</span>
                      </Link>
                    ))}
                  </div>
                  <Link className="action" to={`/funds?module=${module.id}`}>
                    浏览全部基金 <span aria-hidden="true">→</span>
                  </Link>
                </section>
              ))}
          </div>
        </section>
      ))}
    </>
  );
}
