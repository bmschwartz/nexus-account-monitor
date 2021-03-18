import schedule from "node-schedule"
import { Exchange, ExchangeAccount, OperationType, PrismaClient } from "@prisma/client";
import { getAllSettledResults } from "../helper";
import { validateApiKeyAndSecret } from "../repository/ExchangeAccountRepository";
import { MessageClient } from "./messenger";
import { logger } from "../logger";

const HEARTBEAT_TIMEOUT = 2 * 60 * 1000 // ms

function _checkAccountLife(prisma: PrismaClient, messenger: MessageClient) {
  return async () => {
    const timeoutDate = new Date(Date.now() - HEARTBEAT_TIMEOUT)
    const timedOutAccounts = await prisma.exchangeAccount.findMany({
      where: { active: true, lastHeartbeat: { lt: timeoutDate } },
    })

    getAllSettledResults(await Promise.allSettled(
      timedOutAccounts
        .map(recreateAccount(prisma, messenger))
        .filter(Boolean),
    ))
  }
}

function recreateAccount(prisma: PrismaClient, messenger: MessageClient): (account: ExchangeAccount) => Promise<string | undefined> {
  return async ({ id: accountId, exchange, apiKey, apiSecret }: ExchangeAccount) => {
    let opType: OperationType
    switch (exchange) {
      case Exchange.BITMEX:
        opType = OperationType.CREATE_BITMEX_ACCOUNT
        break
      case Exchange.BINANCE:
        opType = OperationType.CREATE_BINANCE_ACCOUNT
        break
    }

    const query = `
    SELECT count(*) FROM "AsyncOperation"
    WHERE
      "opType" = '${opType}' AND
      payload ->> 'accountId' = '${accountId}' AND
      complete = false;`
    const pendingCreateOpCount = await prisma.$queryRaw(query)

    if (!pendingCreateOpCount || pendingCreateOpCount[0]["count"] > 0) {
      return
    }

    if (!apiKey || !apiSecret) {
      return
    }

    const isValidApiKeyAndSecret = await validateApiKeyAndSecret(exchange, apiKey, apiSecret)
    if (!isValidApiKeyAndSecret) {
      logger.info({ message: `Invalid API keys [${accountId} for ${exchange}]` })
      await prisma.exchangeAccount.update({ where: { id: accountId }, data: { active: false, updatedAt: new Date() } })
      return
    }

    logger.info({ message: "Sending create account", accountId, exchange })

    try {
      switch (exchange) {
        case Exchange.BITMEX:
          await messenger.sendDeleteBitmexAccount(accountId, false, true)
          return messenger.sendCreateBitmexAccount(accountId, apiKey, apiSecret)
        case Exchange.BINANCE:
          await messenger.sendDeleteBinanceAccount(accountId, true)
          return messenger.sendCreateBinanceAccount(accountId, apiKey, apiSecret)
      }
    } catch (e) {
      logger.info({ message: "Send create account error", accountId })
    }

    return
  }
}

export class AccountMonitor {
  _db: PrismaClient
  _messenger: MessageClient
  _accountMonitorJob: schedule.Job

  constructor(prisma: PrismaClient, messenger: MessageClient) {
    this._db = prisma
    this._messenger = messenger
  }

  start() {
    setTimeout(() => {
      this._accountMonitorJob = schedule.scheduleJob(
        "accountMonitor",
        "*/60 * * * * *", // every 60 seconds
        _checkAccountLife(this._db, this._messenger),
      )
    }, 2 * 60 * 1000)  // wait for a while before starting to check account timeouts
  }
}
