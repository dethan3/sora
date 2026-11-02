export type Instrument = {
  id: string;
  name: string;
  timeZone: 'America/New_York' | 'Asia/Hong_Kong';
  api: string;
  symbol: string;
  sourceUrl: string;
  yahooSymbol: string;
  instrumentType?: 'index' | 'fund-price';
  fundCode?: string;
  unit?: 'points' | 'usd-per-share' | 'hkd-per-share';
};
// Index and product prices have distinct IDs; prices never proxy index observations.
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
  ...[
    ['AIQ', 'Global X 人工智能与科技 ETF'],
    ['SOXX', 'iShares 半导体 ETF'],
    ['IAU', 'iShares 黄金信托'],
    ['QQQ', 'Invesco 纳斯达克100 ETF'],
    ['IVV', 'iShares 标普500 ETF'],
    ['2800.HK', '盈富基金'],
  ].map(([code, name]): Instrument => {
    const hk = code === '2800.HK';
    return {
      id: `fund:${code}`,
      name,
      instrumentType: 'fund-price',
      fundCode: code,
      unit: hk ? 'hkd-per-share' : 'usd-per-share',
      timeZone: hk ? 'Asia/Hong_Kong' : 'America/New_York',
      api: hk ? 'stock_hk_daily' : 'stock_us_daily',
      symbol: hk ? '02800' : code,
      yahooSymbol: code,
      sourceUrl: hk
        ? 'https://stock.finance.sina.com.cn/hkstock/quotes/02800.html'
        : `https://stock.finance.sina.com.cn/usstock/quotes/${code}.html`,
    };
  }),
];
