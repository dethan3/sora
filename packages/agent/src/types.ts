import type { Market, Index, FundAnalysis, ResearchCard } from '@sora/core'
import type { SearchResult } from '@sora/sources'

export interface ResearchCardInput {
  market: Market
  indexes: Index[]
  fundAnalyses: FundAnalysis[]
  searchResults?: SearchResult[]
}

export interface ResearchAgent {
  generateResearchCard(input: ResearchCardInput): Promise<ResearchCard>
  summarizeMarketSignal(signals: string[]): Promise<string>
  explainTransmissionPath(marketId: string): Promise<string>
  generateFollowUpTasks(card: ResearchCard): Promise<string[]>
}
