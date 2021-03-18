import _ from "lodash"
import * as Amqp from "amqp-ts"
import schedule from "node-schedule"
import { PrismaClient, OperationType } from "@prisma/client";
import { SETTINGS } from "../settings";
import { createAsyncOperation, completeAsyncOperation } from "../repository/AsyncOperationRepository";
import { logger } from "../logger";
import { ASYNC_OPERATION_TTL } from "./asyncOpCleanup";

interface AccountOperationResponse {
  success: boolean
  error?: string
  accountId: string
  operationId: string
}

interface HeartbeatResponse {
  accountId: string
}

const accountHeartbeats: object = {}
const heartbeats: Set<string> = new Set<string>()

let _db: PrismaClient

function _flushAccountHeartbeats(prisma: PrismaClient) {
  return async () => {
    await prisma.exchangeAccount.updateMany({
      where: { id: { in: Array.from(heartbeats) } },
      data: { lastHeartbeat: new Date() },
    })

    heartbeats.clear()
  }
}

export class MessageClient {
  _db: PrismaClient
  _recvConn: Amqp.Connection
  _sendConn: Amqp.Connection

  // Command Queues
  _createBinanceAccountQueue?: Amqp.Queue
  _createBitmexAccountQueue?: Amqp.Queue

  // Event Queues
  _binanceAccountHeartbeatQueue?: Amqp.Queue
  _binanceAccountCreatedQueue?: Amqp.Queue
  _binanceAccountUpdatedQueue?: Amqp.Queue
  _binanceAccountDeletedQueue?: Amqp.Queue

  /* Account Queues */
  _bitmexAccountCreatedQueue?: Amqp.Queue
  _bitmexAccountUpdatedQueue?: Amqp.Queue
  _bitmexAccountDeletedQueue?: Amqp.Queue
  _bitmexAccountHeartbeatQueue?: Amqp.Queue

  // Exchanges
  _recvBinanceExchange?: Amqp.Exchange
  _sendBinanceExchange?: Amqp.Exchange

  _recvBitmexExchange?: Amqp.Exchange
  _sendBitmexExchange?: Amqp.Exchange

  _recvGroupExchange?: Amqp.Exchange

  _heartbeatJob: schedule.Job

  constructor(prisma: PrismaClient) {
    _db = prisma
    this._db = prisma
    this._recvConn = new Amqp.Connection(SETTINGS["AMQP_URL"])
    this._sendConn = new Amqp.Connection(SETTINGS["AMQP_URL"])

    this._connectBinanceMessaging()
    this._connectBitmexMessaging()

    this._setupHeartbeatJob()
  }

  async _connectBitmexMessaging() {
    /* Exchanges */
    this._recvBitmexExchange = this._recvConn.declareExchange(SETTINGS["BITMEX_EXCHANGE"], "topic", { durable: true })
    this._sendBitmexExchange = this._sendConn.declareExchange(SETTINGS["BITMEX_EXCHANGE"], "topic", { durable: true })

    /* Command queues */
    this._createBitmexAccountQueue = this._sendConn.declareQueue(SETTINGS["BITMEX_CREATE_ACCOUNT_QUEUE"], { durable: true })

    /* Event queues */
    /* Account Events */
    this._bitmexAccountCreatedQueue = this._recvConn.declareQueue(SETTINGS["BITMEX_ACCOUNT_CREATED_QUEUE"], { durable: true })
    await this._bitmexAccountCreatedQueue.bind(this._recvBitmexExchange, SETTINGS["BITMEX_EVENT_ACCOUNT_CREATED_KEY"])
    await this._bitmexAccountCreatedQueue.activateConsumer(
      async (message: Amqp.Message) => await this._accountCreatedConsumer(this._db, message),
    )

    this._bitmexAccountUpdatedQueue = this._recvConn.declareQueue(SETTINGS["BITMEX_ACCOUNT_UPDATED_QUEUE"], { durable: true })
    await this._bitmexAccountUpdatedQueue.bind(this._recvBitmexExchange, SETTINGS["BITMEX_EVENT_ACCOUNT_UPDATED_KEY"])
    await this._bitmexAccountUpdatedQueue.activateConsumer(
      async (message: Amqp.Message) => await this._accountUpdatedConsumer(this._db, message),
    )

    this._bitmexAccountDeletedQueue = this._recvConn.declareQueue(SETTINGS["BITMEX_ACCOUNT_DELETED_QUEUE"], { durable: true })
    await this._bitmexAccountDeletedQueue.bind(this._recvBitmexExchange, SETTINGS["BITMEX_EVENT_ACCOUNT_DELETED_KEY"])
    await this._bitmexAccountDeletedQueue.activateConsumer(
      async (message: Amqp.Message) => await this._accountDeletedConsumer(this._db, message),
    )

    /* Heartbeat Events */
    this._bitmexAccountHeartbeatQueue = this._recvConn.declareQueue(SETTINGS["BITMEX_ACCOUNT_HEARTBEAT_QUEUE"], { durable: true })
    await this._bitmexAccountHeartbeatQueue.bind(this._recvBitmexExchange, SETTINGS["BITMEX_EVENT_ACCOUNT_HEARTBEAT_KEY"])
    await this._bitmexAccountHeartbeatQueue.activateConsumer(this._accountHeartbeatConsumer)
  }

  async _connectBinanceMessaging() {
    /* Exchanges */
    this._recvBinanceExchange = this._recvConn.declareExchange(SETTINGS["BINANCE_EXCHANGE"], "topic", { durable: true })
    this._sendBinanceExchange = this._sendConn.declareExchange(SETTINGS["BINANCE_EXCHANGE"], "topic", { durable: true })

    /* Command queues */
    this._createBinanceAccountQueue = this._sendConn.declareQueue(SETTINGS["BINANCE_CREATE_ACCOUNT_QUEUE"], { durable: true })

    /* Event queues */
    this._binanceAccountCreatedQueue = this._recvConn.declareQueue(SETTINGS["BINANCE_ACCOUNT_CREATED_QUEUE"], { durable: true })
    await this._binanceAccountCreatedQueue.bind(this._recvBinanceExchange, SETTINGS["BINANCE_EVENT_ACCOUNT_CREATED_KEY"])
    await this._binanceAccountCreatedQueue.activateConsumer(
      async (message: Amqp.Message) => await this._accountCreatedConsumer(this._db, message),
    )

    this._binanceAccountUpdatedQueue = this._recvConn.declareQueue(SETTINGS["BINANCE_ACCOUNT_UPDATED_QUEUE"], { durable: true })
    await this._binanceAccountUpdatedQueue.bind(this._recvBinanceExchange, SETTINGS["BINANCE_EVENT_ACCOUNT_UPDATED_KEY"])
    await this._binanceAccountUpdatedQueue.activateConsumer(
      async (message: Amqp.Message) => await this._accountUpdatedConsumer(this._db, message),
    )

    this._binanceAccountDeletedQueue = this._recvConn.declareQueue(SETTINGS["BINANCE_ACCOUNT_DELETED_QUEUE"], { durable: true })
    await this._binanceAccountDeletedQueue.bind(this._recvBinanceExchange, SETTINGS["BINANCE_EVENT_ACCOUNT_DELETED_KEY"])
    await this._binanceAccountDeletedQueue.activateConsumer(
      async (message: Amqp.Message) => await this._accountDeletedConsumer(this._db, message),
    )

    this._binanceAccountHeartbeatQueue = this._recvConn.declareQueue(SETTINGS["BINANCE_ACCOUNT_HEARTBEAT_QUEUE"], { durable: true })
    await this._binanceAccountHeartbeatQueue.bind(this._recvBinanceExchange, SETTINGS["BINANCE_EVENT_ACCOUNT_HEARTBEAT_KEY"])
    await this._binanceAccountHeartbeatQueue.activateConsumer(this._accountHeartbeatConsumer)
  }

  async _accountHeartbeatConsumer(message: Amqp.Message) {
    const { accountId }: HeartbeatResponse = message.getContent()
    const { correlationId } = message.properties

    if (!accountId) {
      logger.error({ message: "Account ID missing in Account Heartbeat", correlationId })
      message.reject(false)
      return
    }

    accountHeartbeats[accountId] = new Date()
    heartbeats.add(accountId)

    message.ack()
  }

  async _accountCreatedConsumer(prisma: PrismaClient, message: Amqp.Message) {
    const { success, accountId, error }: AccountOperationResponse = message.getContent()
    const { correlationId: operationId } = message.properties

    logger.debug({ message: "[_accountCreatedConsumer] Received message" })

    if (!operationId) {
      logger.error({ message: "[_accountCreatedConsumer] Missing operationId" })
      message.reject(false)
      return
    }

    if (!accountId) {
      logger.error({ message: "[_accountCreatedConsumer] Missing accountId", operationId })
      message.reject(false)
      return
    }

    const op = await completeAsyncOperation(prisma, operationId, success, [error])

    if (!op) {
      message.reject(false)
      return
    }

    try {
      if (success) {
        logger.info({ message: "[_accountCreatedConsumer] Account created", accountId })
        await prisma.exchangeAccount.update({
          where: { id: accountId },
          data: { active: true, lastHeartbeat: new Date(), updatedAt: new Date() },
        })
      } else {
        logger.info({ message: "[_accountCreatedConsumer] Account not created", accountId })
        await prisma.exchangeAccount.update({
          where: { id: accountId },
          data: { active: false, updatedAt: new Date() },
        })
      }
    } catch (e) {
      logger.error({ message: "[_accountCreatedConsumer] ExchangeAccount update error", error: e })
    }

    message.ack()
  }

  async _accountUpdatedConsumer(prisma: PrismaClient, message: Amqp.Message) {
    message.ack()
  }

  async _accountDeletedConsumer(prisma: PrismaClient, message: Amqp.Message) {
    const { success, accountId, error }: AccountOperationResponse = message.getContent()
    const { correlationId: operationId } = message.properties

    logger.debug({ message: "[_accountDeletedConsumer] Received message" })

    if (!operationId) {
      logger.error({ message: "[_accountDeletedConsumer] Missing operationId" })
      message.reject(false)
      return
    }

    const operation = await completeAsyncOperation(prisma, operationId, success, [error])

    if (!operation) {
      message.reject(false)
      return
    }

    try {

      if (operation && accountId && success) {
        logger.info({ message: "[_accountDeletedConsumer] Deleted Account", accountId, opType: operation.opType })
        switch (operation.opType) {
          case OperationType.DELETE_BITMEX_ACCOUNT:
          case OperationType.DELETE_BINANCE_ACCOUNT: {
            await prisma.exchangeAccount.update({
              where: { id: accountId },
              data: {
                active: false,
                apiKey: null,
                apiSecret: null,
                updatedAt: new Date(),
              },
            })
            break;
          }
          case OperationType.DISABLE_BITMEX_ACCOUNT:
          case OperationType.DISABLE_BINANCE_ACCOUNT: {
            await prisma.exchangeAccount.update({
              where: { id: accountId },
              data: { active: false, updatedAt: new Date() },
            })
            break;
          }
          case OperationType.CLEAR_BITMEX_NODE:
          default: {
            break;
          }
        }
      }
    } catch (e) {
      logger.error({ message: "[_accountDeletedConsumer] Update error", error: e })
    }

    message.ack()
  }

  async sendCreateBitmexAccount(accountId: string, apiKey: string, apiSecret: string): Promise<string> {
    const payload = { accountId, apiKey, apiSecret }

    logger.info({
      message: "[sendCreateBitmexAccount] Sending message",
      accountId,
      apiKey: apiKey.length > 0 ? `${apiKey.length} characters` : "Not Present",
      apiSecret: apiSecret.length > 0 ? `${apiSecret.length} characters` : "Not Present",
    })
    const op = await createAsyncOperation(this._db, { payload }, OperationType.CREATE_BITMEX_ACCOUNT)

    if (!op) {
      logger.error({ message: "[sendCreateBitmexAccount] Error creating async op", accountId })
      throw new Error("Could not create asyncOperation")
    }

    const message = new Amqp.Message(JSON.stringify(payload),
      { persistent: true, correlationId: String(op.id), expiration: ASYNC_OPERATION_TTL },
    )
    this._sendBitmexExchange?.send(message, SETTINGS["BITMEX_CREATE_ACCOUNT_CMD_KEY"])

    return op.id
  }

  async sendUpdateBitmexAccount(accountId: string, apiKey: string, apiSecret: string) {
    const payload = { accountId, apiKey, apiSecret }

    logger.info({
      message: "[sendUpdateBitmexAccount] Sending message",
      accountId,
      apiKey: apiKey.length > 0 ? `${apiKey.length} characters` : "Not Present",
      apiSecret: apiSecret.length > 0 ? `${apiSecret.length} characters` : "Not Present",
    })
    const op = await createAsyncOperation(this._db, { payload }, OperationType.UPDATE_BITMEX_ACCOUNT)

    if (!op) {
      logger.error({ message: "[sendUpdateBitmexAccount] Error creating async op", accountId })
      throw new Error("Could not create asyncOperation")
    }

    const message = new Amqp.Message(JSON.stringify(payload),
      { persistent: true, correlationId: String(op.id), expiration: ASYNC_OPERATION_TTL },
    )
    this._sendBitmexExchange?.send(message, `${SETTINGS["BITMEX_UPDATE_ACCOUNT_CMD_KEY_PREFIX"]}${accountId}`)

    return op.id
  }

  async sendDeleteBitmexAccount(accountId: string, disabling: boolean, clearing: boolean) {
    const payload = { accountId }

    let opType: OperationType
    if (disabling) {
      opType = OperationType.DISABLE_BITMEX_ACCOUNT
    } else if (clearing) {
      opType = OperationType.CLEAR_BITMEX_NODE
    } else {
      opType = OperationType.DELETE_BITMEX_ACCOUNT
    }

    logger.info({
      message: "[sendDeleteBitmexAccount] Sending message",
      accountId,
      opType,
    })

    const op = await createAsyncOperation(this._db, { payload }, opType)

    if (!op) {
      logger.error({ message: "[sendDeleteBitmexAccount] Error creating async op", accountId })
      throw new Error("Could not create asyncOperation")
    }

    const message = new Amqp.Message(JSON.stringify(payload),
      { persistent: true, correlationId: String(op.id), expiration: ASYNC_OPERATION_TTL },
    )
    this._sendBitmexExchange?.send(message, `${SETTINGS["BITMEX_DELETE_ACCOUNT_CMD_KEY_PREFIX"]}${accountId}`)

    return op.id
  }

  async sendCreateBinanceAccount(accountId: string, apiKey: string, apiSecret: string): Promise<string> {
    const payload = { accountId, apiKey, apiSecret }

    logger.info({
      message: "[sendCreateBinanceAccount] Sending message",
      accountId,
      apiKey: apiKey.length > 0 ? `${apiKey.length} characters` : "Not Present",
      apiSecret: apiSecret.length > 0 ? `${apiSecret.length} characters` : "Not Present",
    })

    const op = await createAsyncOperation(this._db, { payload }, OperationType.CREATE_BINANCE_ACCOUNT)

    if (!op) {
      logger.error({ message: "[sendCreateBinanceAccount] Error creating async op", accountId })
      throw new Error("Could not create asyncOperation")
    }

    const message = new Amqp.Message(JSON.stringify(payload),
      { persistent: true, correlationId: String(op.id), expiration: ASYNC_OPERATION_TTL },
    )
    this._sendBinanceExchange?.send(message, SETTINGS["BINANCE_CREATE_ACCOUNT_CMD_KEY"])

    return op.id
  }

  async sendDeleteBinanceAccount(accountId: string, disabling?: boolean) {
    const payload = { accountId }

    const opType = disabling ? OperationType.DISABLE_BINANCE_ACCOUNT : OperationType.DELETE_BINANCE_ACCOUNT

    logger.info({
      message: "[sendDeleteBinanceAccount] Sending message",
      accountId,
      opType,
    })

    const op = await createAsyncOperation(this._db, { payload }, opType)

    if (!op) {
      logger.error({ message: "[sendDeleteBinanceAccount] Error creating async op", accountId })
      throw new Error("Could not create asyncOperation")
    }

    const message = new Amqp.Message(JSON.stringify(payload),
      { persistent: true, correlationId: String(op.id), expiration: ASYNC_OPERATION_TTL },
    )
    this._sendBinanceExchange?.send(message, `${SETTINGS["BINANCE_DELETE_ACCOUNT_CMD_KEY_PREFIX"]}${accountId}`)

    return op.id
  }

  async _setupHeartbeatJob() {
    this._heartbeatJob = schedule.scheduleJob(
      "heartbeatJob",
      "*/5 * * * * *", // every */X seconds
      _flushAccountHeartbeats(this._db)
    )
  }
}
