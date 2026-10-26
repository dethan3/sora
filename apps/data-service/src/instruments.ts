export type Instrument = {
  id: string;
  name: string;
  timeZone: 'America/New_York' | 'Asia/Hong_Kong';
  api: string;
  symbol: string;
  sourceUrl: string;
  yahooSymbol: string;
};
// Explicit index mappings only. ETF prices are never used as index proxies.
export const instruments: Instrument[] = [
  {
    id: 'nasdaq100',
    name: 'Nasdaq-100 Index',
    timeZone: 'America/New_York',
    api: 'index_us_stock_sina',
    symbol: '.NDX',
    yahooSymbol: '^NDX',
    sourceUrl: 'https://stock.finance.sina.com.cn/usstock/quotes/.NDX.html',
  },
  {
    id: 'sp500',
    name: 'S&P 500 Index',
    timeZone: 'America/New_York',
    api: 'index_us_stock_sina',
    symbol: '.INX',
    yahooSymbol: '^GSPC',
    sourceUrl: 'https://stock.finance.sina.com.cn/usstock/quotes/.INX.html',
  },
  {
    id: 'hangseng',
    name: '恒生指数',
    timeZone: 'Asia/Hong_Kong',
    api: 'stock_hk_index_daily_sina',
    symbol: 'HSI',
    yahooSymbol: '^HSI',
    sourceUrl: 'https://stock.finance.sina.com.cn/hkstock/quotes/HSI.html',
  },
];
