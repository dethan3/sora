import { Route, Routes } from 'react-router';
import type { DemoData } from '../data/demo';
import { DiscoverPage } from '../features/discover/DiscoverPage';
import { FundsPage } from '../features/funds/FundsPage';
import { FundDetailPage } from '../features/funds/FundDetailPage';
import { WatchlistPage } from '../features/watchlist/WatchlistPage';
import { PortfolioPage } from '../features/portfolio/PortfolioPage';
import { PersonalProvider } from '../features/local/PersonalProvider';
import { LocalNotice } from '../features/local/BackupTools';

export function DataPages({ data }: { data: DemoData }) {
  return (
    <PersonalProvider>
      <LocalNotice />
      <Routes>
        <Route path="/discover" element={<DiscoverPage data={data} />} />
        <Route path="/funds" element={<FundsPage data={data} />} />
        <Route path="/funds/:code" element={<FundDetailPage data={data} />} />
        <Route path="/watchlist" element={<WatchlistPage data={data} />} />
        <Route path="/portfolio" element={<PortfolioPage data={data} />} />
      </Routes>
    </PersonalProvider>
  );
}
