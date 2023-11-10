/* eslint-disable max-len */
import {logger, stats, STATS_NO_SAMPLING} from '@dydxprotocol-indexer/base';
import {
  AssetCreateEventV1, DeleveragingEventV1, FundingEventV1,
  IndexerTendermintBlock,
  IndexerTendermintEvent,
  IndexerTendermintEvent_BlockEvent, LiquidityTierUpsertEventV1,
  MarketEventV1,
  OrderFillEventV1, PerpetualMarketCreateEventV1,
  StatefulOrderEventV1,
  SubaccountUpdateEventV1,
  TransferEventV1, UpdateClobPairEventV1, UpdatePerpetualEventV1
} from '@dydxprotocol-indexer/v4-protos';
import {
  storeHelpers,
} from '@dydxprotocol-indexer/postgres';
import _ from 'lodash';

import { Handler } from '../handlers/handler';
import { AssetValidator } from '../validators/asset-validator';
import { DeleveragingValidator } from '../validators/deleveraging-validator';
import { FundingValidator } from '../validators/funding-validator';
import { LiquidityTierValidator } from '../validators/liquidity-tier-validator';
import { MarketValidator } from '../validators/market-validator';
import { OrderFillValidator } from '../validators/order-fill-validator';
import { PerpetualMarketValidator } from '../validators/perpetual-market-validator';
import { StatefulOrderValidator } from '../validators/stateful-order-validator';
import { SubaccountUpdateValidator } from '../validators/subaccount-update-validator';
import { TransferValidator } from '../validators/transfer-validator';
import { UpdateClobPairValidator } from '../validators/update-clob-pair-validator';
import { UpdatePerpetualValidator } from '../validators/update-perpetual-validator';
import { Validator, ValidatorInitializer } from '../validators/validator';
import { BatchedHandlers } from './batched-handlers';
import { indexerTendermintEventToEventProtoWithType, indexerTendermintEventToTransactionIndex } from './helper';
import { KafkaPublisher } from './kafka-publisher';
import { SyncHandlers, SYNCHRONOUS_SUBTYPES } from './sync-handlers';
import {
  DydxIndexerSubtypes, EventMessage, EventProtoWithTypeAndVersion, GroupedEvents,
} from './types';
import config from '../config';
import * as pg from 'pg';
import {SUBACCOUNT_ORDER_FILL_EVENT_TYPE} from "../constants";

const TXN_EVENT_SUBTYPE_VERSION_TO_VALIDATOR_MAPPING: Record<string, ValidatorInitializer> = {
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.ORDER_FILL.toString(), 1)]: OrderFillValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.SUBACCOUNT_UPDATE.toString(), 1)]: SubaccountUpdateValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.TRANSFER.toString(), 1)]: TransferValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.MARKET.toString(), 1)]: MarketValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.STATEFUL_ORDER.toString(), 1)]: StatefulOrderValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.ASSET.toString(), 1)]: AssetValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.PERPETUAL_MARKET.toString(), 1)]: PerpetualMarketValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.LIQUIDITY_TIER.toString(), 1)]: LiquidityTierValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.UPDATE_PERPETUAL.toString(), 1)]: UpdatePerpetualValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.UPDATE_CLOB_PAIR.toString(), 1)]: UpdateClobPairValidator,
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.DELEVERAGING.toString(), 1)]: DeleveragingValidator,
};

const BLOCK_EVENT_SUBTYPE_VERSION_TO_VALIDATOR_MAPPING: Record<string, ValidatorInitializer> = {
  [serializeSubtypeAndVersion(DydxIndexerSubtypes.FUNDING.toString(), 1)]: FundingValidator,
};

function serializeSubtypeAndVersion(
  subtype: string,
  version: number,
): string {
  return `${subtype}-${version}`;
}

type DecodedIndexerTendermintBlock = Omit<IndexerTendermintBlock, 'events'> & {
  events: DecodedIndexerTendermintEvent[];
}

type DecodedIndexerTendermintEvent = Omit<IndexerTendermintEvent, 'dataBytes'> & {
  /** Decoded tendermint event. */
  dataBytes: object;
}

export class BlockProcessor {
  block: IndexerTendermintBlock;
  txId: number;
  batchedHandlers: BatchedHandlers;
  syncHandlers: SyncHandlers;

  constructor(
    block: IndexerTendermintBlock,
    txId: number,
  ) {
    this.block = block;
    this.txId = txId;
    this.batchedHandlers = new BatchedHandlers();
    this.syncHandlers = new SyncHandlers();
  }

  /**
   * Saves all Tendermint event data to postgres. Performs these operations in the following steps:
   * 1. Group - Groups events by transaction events and block events by transactionIndex and
   *            eventIndex.
   * 2. Validation - Validates that all data from v4 has the required fields with
   *                 Handler.validate(). Validation failure will throw a ParseMessageError.
   * 3. Organize - Groups events into events that can be processed in parallel. Each handler
   *               will generate a list of ids that if matching another handler's ids cannot
   *               be processed in parallel. For example, two SubaccountUpdateEvents for the
   *               same subaccount cannot be processed in parallel because the subaccount's
   *               final balance should be the last SubaccountUpdateEvent's balance.
   * 4. Processing - Based on the groupings created above, process events in each batch in parallel.
   * @returns the kafka publisher which contains all the events to be published to the kafka
   */
  public async process(): Promise<KafkaPublisher> {
    const groupedEvents: GroupedEvents = this.groupEvents();
    this.validateAndOrganizeEvents(groupedEvents);
    return this.processEvents();
  }

  /**
   * Groups events into block events and events for each transactionIndex
   * @param block the IndexerTendermintBlock to group events
   * @returns
   */
  private groupEvents(): GroupedEvents {
    const groupedEvents: GroupedEvents = {
      transactionEvents: [],
      blockEvents: [],
    };

    for (let i: number = 0; i < this.block.txHashes.length; i++) {
      groupedEvents.transactionEvents.push([]);
    }

    for (let i: number = 0; i < this.block.events.length; i++) {
      const event: IndexerTendermintEvent = this.block.events[i];
      const transactionIndex: number = indexerTendermintEventToTransactionIndex(event);
      const eventProtoWithType:
      EventProtoWithTypeAndVersion | undefined = indexerTendermintEventToEventProtoWithType(
        i,
        event,
      );
      if (eventProtoWithType === undefined) {
        continue;
      }
      eventProtoWithType.blockEventIndex = i;
      if (transactionIndex === -1) {
        groupedEvents.blockEvents.push(eventProtoWithType);
        continue;
      }

      groupedEvents.transactionEvents[transactionIndex].push(eventProtoWithType);
    };
    return groupedEvents;
  }

  /**
   * Organizes all events into batches that can be processed in parallel, and validates that
   * the blocks are valid. Any invalid block will throw a ParseMessageError, and will be handled
   * in onMessage.
   * Each event is validated by a validator and also returns a list of handlers that will process
   * the event.
   */
  private validateAndOrganizeEvents(groupedEvents: GroupedEvents): void {
    for (const eventsInTransaction of groupedEvents.transactionEvents) {
      for (const eventProtoWithType of eventsInTransaction) {
        this.validateAndAddHandlerForEvent(
          eventProtoWithType,
          TXN_EVENT_SUBTYPE_VERSION_TO_VALIDATOR_MAPPING,
        );
      }
    }
    for (const eventProtoWithType of groupedEvents.blockEvents) {
      this.validateAndAddHandlerForEvent(
        eventProtoWithType,
        BLOCK_EVENT_SUBTYPE_VERSION_TO_VALIDATOR_MAPPING,
      );
    }
  }

  private validateAndAddHandlerForEvent(
    eventProto: EventProtoWithTypeAndVersion,
    validatorMap: Record<string, ValidatorInitializer>,
  ): void {
    const Initializer:
    ValidatorInitializer | undefined = validatorMap[
      serializeSubtypeAndVersion(
        eventProto.type,
        eventProto.version,
      )
    ];
    if (Initializer === undefined) {
      const message: string = `cannot process subtype ${eventProto.type} and version ${eventProto.version}`;
      logger.error({
        at: 'onMessage#saveTendermintEventData',
        message,
        eventProto,
      });
      return;
    }

    const validator: Validator<EventMessage> = new Initializer(
      eventProto.eventProto,
      this.block,
      eventProto.blockEventIndex,
    );

    validator.validate();
    const handlers: Handler<EventMessage>[] = validator.createHandlers(
      eventProto.indexerTendermintEvent,
      this.txId,
    );

    _.map(handlers, (handler: Handler<EventMessage>) => {
      if (SYNCHRONOUS_SUBTYPES.includes(eventProto.type as DydxIndexerSubtypes)) {
        this.syncHandlers.addHandler(eventProto.type, handler);
      } else {
        this.batchedHandlers.addHandler(handler);
      }
    });
  }

  private async processEvents(): Promise<KafkaPublisher> {
    const kafkaPublisher: KafkaPublisher = new KafkaPublisher();
    // in genesis, handle sync events first, then batched events.
    // in other blocks, handle batched events first, then sync events.
    let resultRow: pg.QueryResultRow | undefined = undefined;
    if (config.USE_BLOCK_PROCESSOR_SQL_FUNCTION) {
      const decodedBlock: DecodedIndexerTendermintBlock = {
        ...this.block,
        events: [],
      }
      for (let event of this.block.events) {
        switch (event.subtype) {
          case DydxIndexerSubtypes.ORDER_FILL:
            decodedBlock.events.push({
              ...event,
              dataBytes: OrderFillEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.SUBACCOUNT_UPDATE:
            decodedBlock.events.push({
              ...event,
              dataBytes: SubaccountUpdateEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.TRANSFER:
            decodedBlock.events.push({
              ...event,
              dataBytes: TransferEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.MARKET:
            decodedBlock.events.push({
              ...event,
              dataBytes: MarketEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.STATEFUL_ORDER:
            decodedBlock.events.push({
              ...event,
              dataBytes: StatefulOrderEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.FUNDING:
            decodedBlock.events.push({
              ...event,
              dataBytes: FundingEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.ASSET:
            decodedBlock.events.push({
              ...event,
              dataBytes: AssetCreateEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.PERPETUAL_MARKET:
            decodedBlock.events.push({
              ...event,
              dataBytes: PerpetualMarketCreateEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.LIQUIDITY_TIER:
            decodedBlock.events.push({
              ...event,
              dataBytes: LiquidityTierUpsertEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.UPDATE_PERPETUAL:
            decodedBlock.events.push({
              ...event,
              dataBytes: UpdatePerpetualEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.UPDATE_CLOB_PAIR:
            decodedBlock.events.push({
              ...event,
              dataBytes: UpdateClobPairEventV1.decode(event.dataBytes),
            });
            break;
          case DydxIndexerSubtypes.DELEVERAGING:
            decodedBlock.events.push({
              ...event,
              dataBytes: DeleveragingEventV1.decode(event.dataBytes),
            });
            break;
        }
      }
      const startQuery: number = Date.now();
     const query: string =           `SELECT dydx_block_processor(
        '${JSON.stringify(decodedBlock)}' 
      ) AS result;`;
      stats.timing(
          `${config.SERVICE_NAME}.processed_block_query_conversion.timing`,
          Date.now() - startQuery,
          STATS_NO_SAMPLING,
          {success: true.toString()},
      );
      const start: number = Date.now();
      let success = false;
     try {
       const result: pg.QueryResult = await storeHelpers.rawQuery(
           query,
           {txId: this.txId},
       ).catch((error: Error) => {
         logger.error({
           at: 'BlockProcessor#processEvents',
           message: 'Failed to handle IndexerTendermintBlock',
           error,
         });
         throw error;
       });
       resultRow = result.rows[0].result;
       success = true;
     } finally {
       stats.timing(
           `${config.SERVICE_NAME}.processed_block_sql.timing`,
           Date.now() - start,
           STATS_NO_SAMPLING,
           {success: success.toString()},
       );
     }
    }

    if (this.block.height === 0) {
      await this.syncHandlers.process(kafkaPublisher, resultRow);
      await this.batchedHandlers.process(kafkaPublisher, resultRow);
    } else {
      await this.batchedHandlers.process(kafkaPublisher, resultRow);
      await this.syncHandlers.process(kafkaPublisher, resultRow);
    }
    return kafkaPublisher;
  }
}
