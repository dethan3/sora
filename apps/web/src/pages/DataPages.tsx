import { Route, Routes } from 'react-router';
import type { DemoData } from '../data/demo';
import { DiscoverPage } from '../features/discover/DiscoverPage';
import { FundsPage } from '../features/funds/FundsPage';
import { FundDetailPage } from '../features/funds/FundDetailPage';
import { WatchlistPage } from '../features/watchlist/WatchlistPage';
import { PortfolioPage } from '../features/portfolio/PortfolioPage';
import { LocalNotice } from '../features/local/BackupTools';
import { DirectionDetailPage } from '../features/discover/DirectionDetailPage';
import { DirectionNotice } from '../features/discover/DirectionProvider';

export function DataPages({ data }: { data: DemoData }) {
  return (
    <>
      <LocalNotice />
      <DirectionNotice />
      <Routes>
        <Route path="/discover" element={<DiscoverPage data={data} />} />
        <Route
          path="/discover/:id"
          element={<DirectionDetailPage data={data} />}
        />
        <Route path="/funds" element={<FundsPage data={data} />} />
        <Route path="/funds/:code" element={<FundDetailPage data={data} />} />
        <Route path="/watchlist" element={<WatchlistPage data={data} />} />
        <Route path="/portfolio" element={<PortfolioPage data={data} />} />
      </Routes>
    </>
  );
}
