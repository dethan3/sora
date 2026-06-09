import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import type {
  AssetExposure,
  Fund,
  FundMapping,
  FundMetricsSnapshot,
  Thesis,
  ThesisEvidence,
  ThesisUpdate,
} from '@sora/core'
import type { Market } from '@sora/core'
import type { Index } from '@sora/core'
import type { SoraDb } from './db.js'
import {
  markets,
  indexes,
  funds,
  fundIndexMappings,
  fundMetricsSnapshots,
  assetExposures,
  theses,
  thesisEvidence,
  thesisUpdates,
} from './schema.js'

export interface SeedStats {
  markets: number
  indexes: number
  funds: number
  mappings: number
  metrics: number
  theses: number
  thesisEvidence: number
  thesisUpdates: number
  assetExposures: number
}

function readJson<T>(seedsDir: string, filename: string): T[] {
  const content = readFileSync(join(seedsDir, filename), 'utf-8')
  return JSON.parse(content) as T[]
}

export function dbSeed(db: SoraDb, seedsDir: string): SeedStats {
  const marketsData = readJson<Market>(seedsDir, 'markets.json')
  const indexesData = readJson<Index>(seedsDir, 'indexes.json')
  const fundsData = readJson<Fund>(seedsDir, 'funds.json')
  const mappingsData = readJson<FundMapping>(seedsDir, 'mappings.json')
  const metricsData = readJson<FundMetricsSnapshot>(seedsDir, 'fund-metrics.json')
  const thesesData = readJson<Thesis>(seedsDir, 'theses.json')
  const thesisEvidenceData = readJson<ThesisEvidence>(seedsDir, 'thesis-evidence.json')
  const thesisUpdatesData = readJson<ThesisUpdate>(seedsDir, 'thesis-updates.json')
  const assetExposuresData = readJson<AssetExposure>(seedsDir, 'asset-exposures.json')

  for (const m of marketsData) {
    db.insert(markets).values({
      id: m.id,
      name: m.name,
      category: m.category,
      description: m.description ?? null,
    }).onConflictDoUpdate({
      target: markets.id,
      set: {
        name: m.name,
        category: m.category,
        description: m.description ?? null,
      },
    }).run()
  }

  for (const i of indexesData) {
    db.insert(indexes).values({
      id: i.id,
      name: i.name,
      marketId: i.marketId,
      ticker: i.ticker,
      description: i.description ?? null,
    }).onConflictDoUpdate({
      target: indexes.id,
      set: {
        name: i.name,
        marketId: i.marketId,
        ticker: i.ticker,
        description: i.description ?? null,
      },
    }).run()
  }

  for (const f of fundsData) {
    db.insert(funds).values({
      id: f.id,
      fundCode: f.fundCode,
      fundName: f.fundName,
      fundType: f.fundType,
      marketId: f.marketId,
      trackingIndexId: f.trackingIndexId,
      manager: f.manager ?? null,
      fee: f.fee ?? null,
      scale: f.scale ?? null,
      inceptionDate: f.inceptionDate ?? null,
      isEtf: f.isEtf,
      isEtfFeeder: f.isEtfFeeder,
      isQdii: f.isQdii,
      purchaseStatus: f.purchaseStatus,
      purchaseLimit: f.purchaseLimit ?? null,
      dataSource: f.dataSource,
      updatedAt: f.updatedAt,
    }).onConflictDoUpdate({
      target: funds.id,
      set: {
        fundCode: f.fundCode,
        fundName: f.fundName,
        fundType: f.fundType,
        marketId: f.marketId,
        trackingIndexId: f.trackingIndexId,
        manager: f.manager ?? null,
        fee: f.fee ?? null,
        scale: f.scale ?? null,
        inceptionDate: f.inceptionDate ?? null,
        isEtf: f.isEtf,
        isEtfFeeder: f.isEtfFeeder,
        isQdii: f.isQdii,
        purchaseStatus: f.purchaseStatus,
        purchaseLimit: f.purchaseLimit ?? null,
        dataSource: f.dataSource,
        updatedAt: f.updatedAt,
      },
    }).run()
  }

  for (const map of mappingsData) {
    db.insert(fundIndexMappings).values({
      id: map.id,
      fundId: map.fundId,
      indexId: map.indexId,
      isPrimary: map.isPrimary,
    }).onConflictDoUpdate({
      target: fundIndexMappings.id,
      set: {
        fundId: map.fundId,
        indexId: map.indexId,
        isPrimary: map.isPrimary,
      },
    }).run()
  }

  for (const m of metricsData) {
    db.insert(fundMetricsSnapshots).values({
      id: m.id,
      fundId: m.fundId,
      nav: m.nav ?? null,
      price: m.price ?? null,
      premiumRate: m.premiumRate ?? null,
      volume: m.volume ?? null,
      turnover: m.turnover ?? null,
      sharpeRatio: m.sharpeRatio ?? null,
      maxDrawdown: m.maxDrawdown ?? null,
      volatility: m.volatility ?? null,
      trackingError: m.trackingError ?? null,
      return1m: m.return1m ?? null,
      return3m: m.return3m ?? null,
      return6m: m.return6m ?? null,
      return1y: m.return1y ?? null,
      return3y: m.return3y ?? null,
      snapshotDate: m.snapshotDate,
      dataSource: m.dataSource,
    }).onConflictDoUpdate({
      target: fundMetricsSnapshots.id,
      set: {
        fundId: m.fundId,
        nav: m.nav ?? null,
        price: m.price ?? null,
        premiumRate: m.premiumRate ?? null,
        volume: m.volume ?? null,
        turnover: m.turnover ?? null,
        sharpeRatio: m.sharpeRatio ?? null,
        maxDrawdown: m.maxDrawdown ?? null,
        volatility: m.volatility ?? null,
        trackingError: m.trackingError ?? null,
        return1m: m.return1m ?? null,
        return3m: m.return3m ?? null,
        return6m: m.return6m ?? null,
        return1y: m.return1y ?? null,
        return3y: m.return3y ?? null,
        snapshotDate: m.snapshotDate,
        dataSource: m.dataSource,
      },
    }).run()
  }

  for (const t of thesesData) {
    db.insert(theses).values({
      id: t.id,
      title: t.title,
      summary: t.summary,
      timeHorizon: t.timeHorizon,
      status: t.status,
      confidence: t.confidence,
      causalChain: JSON.stringify(t.causalChain),
      keyAssumptions: JSON.stringify(t.keyAssumptions),
      affectedMarketIds: JSON.stringify(t.affectedMarketIds),
      affectedIndexIds: JSON.stringify(t.affectedIndexIds),
      affectedFundIds: JSON.stringify(t.affectedFundIds),
      invalidationConditions: JSON.stringify(t.invalidationConditions),
      createdAt: t.createdAt,
      updatedAt: t.updatedAt,
    }).onConflictDoUpdate({
      target: theses.id,
      set: {
        title: t.title,
        summary: t.summary,
        timeHorizon: t.timeHorizon,
        status: t.status,
        confidence: t.confidence,
        causalChain: JSON.stringify(t.causalChain),
        keyAssumptions: JSON.stringify(t.keyAssumptions),
        affectedMarketIds: JSON.stringify(t.affectedMarketIds),
        affectedIndexIds: JSON.stringify(t.affectedIndexIds),
        affectedFundIds: JSON.stringify(t.affectedFundIds),
        invalidationConditions: JSON.stringify(t.invalidationConditions),
        createdAt: t.createdAt,
        updatedAt: t.updatedAt,
      },
    }).run()
  }

  for (const evidence of thesisEvidenceData) {
    db.insert(thesisEvidence).values({
      id: evidence.id,
      thesisId: evidence.thesisId,
      source: evidence.source,
      title: evidence.title,
      summary: evidence.summary,
      url: evidence.url ?? null,
      direction: evidence.direction,
      strength: evidence.strength,
      confidenceDelta: evidence.confidenceDelta,
      rationale: evidence.rationale,
      observedAt: evidence.observedAt,
      createdAt: evidence.createdAt,
    }).onConflictDoUpdate({
      target: thesisEvidence.id,
      set: {
        thesisId: evidence.thesisId,
        source: evidence.source,
        title: evidence.title,
        summary: evidence.summary,
        url: evidence.url ?? null,
        direction: evidence.direction,
        strength: evidence.strength,
        confidenceDelta: evidence.confidenceDelta,
        rationale: evidence.rationale,
        observedAt: evidence.observedAt,
        createdAt: evidence.createdAt,
      },
    }).run()
  }

  for (const update of thesisUpdatesData) {
    db.insert(thesisUpdates).values({
      id: update.id,
      thesisId: update.thesisId,
      previousConfidence: update.previousConfidence,
      newConfidence: update.newConfidence,
      evidenceIds: JSON.stringify(update.evidenceIds),
      rationale: update.rationale,
      createdAt: update.createdAt,
    }).onConflictDoUpdate({
      target: thesisUpdates.id,
      set: {
        thesisId: update.thesisId,
        previousConfidence: update.previousConfidence,
        newConfidence: update.newConfidence,
        evidenceIds: JSON.stringify(update.evidenceIds),
        rationale: update.rationale,
        createdAt: update.createdAt,
      },
    }).run()
  }

  for (const exposure of assetExposuresData) {
    db.insert(assetExposures).values({
      id: exposure.id,
      thesisId: exposure.thesisId,
      assetType: exposure.assetType,
      assetId: exposure.assetId,
      exposureScore: exposure.exposureScore,
      rationale: exposure.rationale,
      updatedAt: exposure.updatedAt,
    }).onConflictDoUpdate({
      target: assetExposures.id,
      set: {
        thesisId: exposure.thesisId,
        assetType: exposure.assetType,
        assetId: exposure.assetId,
        exposureScore: exposure.exposureScore,
        rationale: exposure.rationale,
        updatedAt: exposure.updatedAt,
      },
    }).run()
  }

  return {
    markets: marketsData.length,
    indexes: indexesData.length,
    funds: fundsData.length,
    mappings: mappingsData.length,
    metrics: metricsData.length,
    theses: thesesData.length,
    thesisEvidence: thesisEvidenceData.length,
    thesisUpdates: thesisUpdatesData.length,
    assetExposures: assetExposuresData.length,
  }
}
