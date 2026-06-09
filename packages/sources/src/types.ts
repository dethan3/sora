export interface IndexQuote {
  ticker: string
  price: number
  changePercent: number
  volume: number | null
  high52w: number | null
  low52w: number | null
  fetchedAt: string
}

export interface IndexHistoricalQuote {
  date: string
  open: number
  high: number
  low: number
  close: number
  volume: number | null
}

export interface RawFundDetails {
  fundCode: string
  fundName: string
  nav: number
  navDate: string
  estimatedNav: number | null
  estimatedChangePercent: number | null
  dataSource: string
  fetchedAt: string
}

export interface RawNavRecord {
  date: string
  nav: number
  changePercent: number | null
}

export interface RawFundMetrics {
  fundCode: string
  nav: number | null
  price: number | null
  premiumRate: number | null
  volume: number | null
  turnover: number | null
  snapshotDate: string
  dataSource: string
}

export interface SearchResult {
  title: string
  url: string
  content: string
  score: number | null
  publishedDate: string | null
}

export interface IMarketQuoteSource {
  getIndexQuote(ticker: string): Promise<IndexQuote>
  getIndexHistory(ticker: string, days: number): Promise<IndexHistoricalQuote[]>
}

export interface IFundDataSource {
  getFundDetails(fundCode: string): Promise<RawFundDetails>
  getFundNavHistory(fundCode: string, days: number): Promise<RawNavRecord[]>
  getFundMetrics(fundCode: string): Promise<RawFundMetrics>
}

export interface ISearchSource {
  search(query: string, maxResults?: number): Promise<SearchResult[]>
}
