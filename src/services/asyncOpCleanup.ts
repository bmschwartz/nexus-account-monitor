import schedule from "node-schedule"
import { PrismaClient, OperationType } from "@prisma/client";

export const ASYNC_OPERATION_TTL = 10 * 60 * 1000 // ms

export class AsyncOpCleanupClient {
  _db: PrismaClient
  _cleanUpAccountAsyncOpJob: schedule.Job

  constructor(prisma: PrismaClient) {
    this._db = prisma
  }

  start() {
    this._cleanUpAccountAsyncOpJob = schedule.scheduleJob(
      "cleanupAccountAsyncOp",
      "* * * * *",
      this._cleanUpAccountAsyncOps(this._db),
    )
  }

  _cleanUpAccountAsyncOps(prisma: PrismaClient) {
    return async () => {
      const expiredCutoffDate = new Date(Date.now() - ASYNC_OPERATION_TTL)
      await prisma.asyncOperation.deleteMany({
        where: {
          opType: {
            in: [
              // Create account
              OperationType.CREATE_BITMEX_ACCOUNT,
              OperationType.CREATE_BINANCE_ACCOUNT,

              // Update account
              OperationType.UPDATE_BITMEX_ACCOUNT,
              OperationType.UPDATE_BINANCE_ACCOUNT,

              // Delete account
              OperationType.DELETE_BITMEX_ACCOUNT,
              OperationType.DELETE_BINANCE_ACCOUNT,

              // Clear node
              OperationType.CLEAR_BITMEX_NODE,
            ],
          },
          createdAt: { lte: expiredCutoffDate },
        },
      })
    }
  }
}
