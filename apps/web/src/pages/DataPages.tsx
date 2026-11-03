import { Route, Routes, useSearchParams } from 'react-router';
import type { DemoData } from '../data/demo';
import { DiscoverPage } from '../features/discover/DiscoverPage';
import { FundsPage } from '../features/funds/FundsPage';
import { FundDetailPage } from '../features/funds/FundDetailPage';
import { WatchlistPage } from '../features/watchlist/WatchlistPage';
import { PortfolioPage } from '../features/portfolio/PortfolioPage';
import { LocalNotice } from '../features/local/BackupTools';
import { DirectionDetailPage } from '../features/discover/DirectionDetailPage';
import { DirectionNotice } from '../features/discover/DirectionProvider';
import { MarketProvider } from '../features/market/MarketProvider';
import { ResearchHubPage, ResearchPage } from '../features/research/Research';
import { DomainPage } from '../features/domains/DomainPage';

function Discovery({ data }: { data: DemoData }) {
  const [params] = useSearchParams();
  const legacy =
    params.get('view') === 'markets' ||
    ['q', 'market', 'kind'].some((key) => params.has(key));
  return legacy ? <DiscoverPage data={data} /> : <DomainPage data={data} />;
}

export function DataPages({ data }: { data: DemoData }) {
  return (
    <MarketProvider>
      <LocalNotice />
      <DirectionNotice />
      <Routes>
        <Route path="/research" element={<ResearchHubPage />} />
        <Route path="/research/:id" element={<ResearchPage data={data} />} />
        <Route path="/discover" element={<Discovery data={data} />} />
        <Route
          path="/discover/:id"
          element={<DirectionDetailPage data={data} />}
        />
        <Route path="/funds" element={<FundsPage data={data} />} />
        <Route path="/funds/:code" element={<FundDetailPage data={data} />} />
        <Route path="/watchlist" element={<WatchlistPage data={data} />} />
        <Route path="/following" element={<WatchlistPage data={data} />} />
        <Route path="/portfolio" element={<PortfolioPage data={data} />} />
      </Routes>
    </MarketProvider>
  );
}
